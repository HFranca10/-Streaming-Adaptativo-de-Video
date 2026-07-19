"""
x.py --politica 2 --segmentos 30 --proxy 1
"""


import json
import time
import csv
import argparse
import os
import sys
from datetime import datetime, timezone
import socket


# =============================================================================
# CONSTANTES;
# =============================================================================

HOST_MANIFEST     = "137.131.178.229"
PORT_MANIFEST     = 8080
FATOR_SEGURANCA   = 0.95
JANELA_TAXA      = 5
TIMEOUT_HTTP_S    = 10
BUFFER_MIN_PLAY_S = 2
CHUNK_SIZE        = 4096   # bytes lidos por vez do socket

# Política 2 — Slow-Start + Buffer-Based híbrido com histerese
DEGRAUS_PARA_SUBIR   = 1    # confirmações para subir 1 degrau na fase de arranque
BUFFER_TRANSICAO_S   = 5.0  # buffer mínimo para sair da fase de arranque e entrar no modo buffer
BUFFER_SUBIR_S       = 8.5  # buffer >= este valor -> sobe 1 degrau (estado normal)
BUFFER_DESCER_S      = 2.3  # buffer <  este valor -> desce 1 degrau
BUFFER_DESCE_1_S     = 4.0  # buffer < este valor -> desce 1 degrau; 
BUFFER_HISTERESE_S   = 2.0  # somado a BUFFER_SUBIR_S apos uma descida; exige folga extra
BUFFER_MAX_S        = 25.0  # nível máximo do buffer para evitar crescimento infinito em redes muito rápidas
                            

# Failover
MAX_FALHAS_CONSECUTIVAS = 2    # falhas consecutivas antes de tentar failover
TIMEOUT_HEALTH_S        = 2    # timeout do health check — deve ser curto; um servidor
                               # vivo responde em < 1s, não vale esperar mais que isso

#Jitter
ALPHA_EWMA     = 0.35    # peso da medição mais recente no EWMA de taxa; valor entre 0 e 1
FATOR_JITTER  = 1.40        # intensidade da penalidade de jitter
                           # taxa_penalizada = ewma * (1 - 0.5 * jitter_s)
                           # ex: jitter=200ms -> penalidade de 10%
                           #     jitter=500ms -> penalidade de 25%
                           #     jitter=1000ms -> penalidade de 50% (máximo)

# =============================================================================
# HTTP SOBRE SOCKET TCP PURO
# =============================================================================

def http_get(host: str, port: int, path: str, timeout: float = TIMEOUT_HTTP_S) -> dict:
    """
    Faz um HTTP GET usando socket TCP puro.

    Retorna dict com:
        corpo        -> bytes do corpo da resposta
        status       -> código HTTP (ex: 200)
        bytes_totais -> tamanho do corpo em bytes
        tempo_s      -> tempo total de download
        tempos_chunks -> timestamps de chegada de cada chunk (para jitter)
        erro         -> None ou mensagem de erro
    """
    requisicao = (
        f"GET {path} HTTP/1.0\r\n"
        f"Host: {host}:{port}\r\n"
        f"Connection: close\r\n"
        f"\r\n"
    ).encode("utf-8")

    resposta_bytes = b""
    tempos_chunks  = []

    t_inicio = time.monotonic()

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
        sock.sendall(requisicao)

        while True:
            chunk = sock.recv(CHUNK_SIZE)
            if not chunk:
                break
            resposta_bytes += chunk
            tempos_chunks.append(time.monotonic())

        sock.close()

    except Exception as e:
        return {
            "corpo":         b"",
            "status":        0,
            "bytes_totais":  0,
            "tempo_s":       time.monotonic() - t_inicio,
            "tempos_chunks": [],
            "erro":          str(e),
        }

    t_fim   = time.monotonic()
    tempo_s = t_fim - t_inicio

    separador = b"\r\n\r\n"
    if separador in resposta_bytes:
        cabecalho_bytes, corpo_bytes = resposta_bytes.split(separador, 1)
    else:
        cabecalho_bytes = resposta_bytes
        corpo_bytes     = b""

    primeira_linha = cabecalho_bytes.split(b"\r\n")[0].decode("utf-8", errors="ignore")
    try:
        status = int(primeira_linha.split(" ")[1])
    except (IndexError, ValueError):
        status = 0

    return {
        "corpo":         corpo_bytes,
        "status":        status,
        "bytes_totais":  len(corpo_bytes),
        "tempo_s":       tempo_s,
        "tempos_chunks": tempos_chunks,
        "erro":          None,
    }

def calcular_metricas(resultado: dict) -> dict:
    """A partir do resultado bruto do http_get, calcula taxa e jitter de rede."""
    bytes_totais  = resultado["bytes_totais"]
    tempo_s       = resultado["tempo_s"]
    tempos_chunks = resultado["tempos_chunks"]

    taxa_kbps = (bytes_totais * 8) / (tempo_s * 1000) if tempo_s > 0 else 0.0

    jitter_network_ms = 0.0
    if len(tempos_chunks) >= 2:
        intervalos = [
            (tempos_chunks[i] - tempos_chunks[i - 1]) * 1000
            for i in range(1, len(tempos_chunks))
        ]
        media             = sum(intervalos) / len(intervalos)
        jitter_network_ms = sum(abs(x - media) for x in intervalos) / len(intervalos)

    return {
        "bytes_totais":      bytes_totais,
        "tempo_s":           tempo_s,
        "taxa_kbps":        taxa_kbps,
        "jitter_network_ms": jitter_network_ms,
        "erro":              resultado["erro"],
    }


# =============================================================================
# FAILOVER MANAGER
# =============================================================================

class FailoverManager:
    """
    Gerencia a lista de servidores e o failover automático por prioridade.

    Fluxo:
      - Registra falhas consecutivas no servidor atual.
      - Quando falhas >= MAX_FALHAS_CONSECUTIVAS, tenta health check nos
        servidores seguintes (por prioridade) e migra para o primeiro
        que responder 200.
      - Registra o total de failovers ocorridos.
    """

    def __init__(self, servidores: list):
        # ordena por prioridade (menor número = maior prioridade)
        self._servidores      = sorted(servidores, key=lambda s: s["priority"])
        self._idx_atual       = 0
        self._falhas_consec   = 0
        self.failover_total   = 0

    @property
    def servidor_atual(self) -> dict:
        return self._servidores[self._idx_atual]

    def _parse_url(self, url: str):
        """Extrai (host, port) de uma URL do tipo 'http://host:port'."""
        limpa = url.replace("http://", "")
        host, port_str = limpa.split(":")
        return host, int(port_str)

    def registrar_sucesso(self):
        """Reseta o contador de falhas do servidor atual."""
        self._falhas_consec = 0

    def registrar_falha(self) -> bool:
        """
        Incrementa falhas consecutivas.
        Retorna True se o limite foi atingido e deve-se tentar failover.
        """
        self._falhas_consec += 1
        return self._falhas_consec >= MAX_FALHAS_CONSECUTIVAS

    def tentar_failover(self) -> bool:
        """
        Percorre os servidores seguintes (por prioridade) e migra para o
        primeiro que passar no health check (GET /health retorna 200).

        Retorna True se o failover teve sucesso, False se nenhum alternativo
        está disponível.
        """
        # range(1, N) garante que nunca testa o servidor atual.
        # O % len() faz o índice dar a volta — com dois servidores:
        #   - se está no A (idx=0): testa B (idx=1)
        #   - se está no B (idx=1): testa A (idx=0)  <- retorno ao A
        # O retorno ao A só ocorre se ele passar no health check.
        # Se A ainda estiver fora, health check falha, retorna False,
        # e o cliente permanece no B até a próxima falha.
        for offset in range(1, len(self._servidores)):
            idx_candidato = (self._idx_atual + offset) % len(self._servidores)
            candidato     = self._servidores[idx_candidato]
            host, port    = self._parse_url(candidato["url"])

            print(f" \n [failover] Testando health check em {candidato['id']} ({candidato['url']}) ...")
            resultado = http_get(host, port, "/health", timeout=TIMEOUT_HEALTH_S)

            if resultado["status"] == 200:
                self._idx_atual     = idx_candidato
                self._falhas_consec = 0
                self.failover_total += 1
                print(f"  [failover] Migrado para {self.servidor_atual['id']} "
                      f"(failover #{self.failover_total})")
                return True
            else:
                print(f"  [failover] {candidato['id']} indisponível "
                      f"(status={resultado['status']}, erro={resultado['erro']})")

        print("  [failover] Nenhum servidor alternativo disponível.")
        return False

    def url_para_segmento(self, url_path: str):
        """Retorna (host, port) do servidor atual."""
        return self._parse_url(self.servidor_atual["url"])


# =============================================================================
# POLÍTICA 1 — ABR RATE-BASED (BASELINE)
# =============================================================================

class AbrRateBased:
    """
    Seleciona a maior representação cujo bitrate_kbps <= taxa_estimada * fator_seguranca.
    A taxa estimada é a média simples dos últimos JANELA_TAXA segmentos.
    """

    nome = "rate_based"

    def __init__(self, representacoes: list):
        self._historico_taxa = []
        self.representacoes = sorted(
            representacoes, key=lambda r: r["bitrate_kbps"], reverse=True
        )

    def registrar_taxa(self, taxa_kbps: float):
        self._historico_taxa.append(taxa_kbps)
        if len(self._historico_taxa) > JANELA_TAXA:
            self._historico_taxa.pop(0)

    def taxa_estimada(self) -> float:
        if not self._historico_taxa:
            return 0.0
        return sum(self._historico_taxa) / len(self._historico_taxa)

    def selecionar_qualidade(self, buffer_nivel_s: float = 0.0) -> dict:
        # buffer_nivel_s ignorado nesta política (Rate-Based puro)
        estimativa = self.taxa_estimada() * FATOR_SEGURANCA
        if estimativa == 0:
            return self.representacoes[-1]   # sem histórico: qualidade mínima
        for rep in self.representacoes:
            if rep["bitrate_kbps"] <= estimativa:
                return rep
        return self.representacoes[-1]


# =============================================================================
# POLÍTICA 2 — BUFFER-BASED
# =============================================================================

class AbrBufferBased:
    """
    Política 2 — Arranque guiado por Taxa com transição para Buffer-Based e histerese.

    Deficiências do baseline que esta política endereça:
    1. O baseline pode escolher qualidade alta no 1.o segmento, quando o
        histórico de taxa ainda é inexistente, causando rebuffering imediato.
    2. O baseline ignora o estado do buffer, que é o indicador mais direto
        da experiência do usuário.
    3. O baseline oscila rapidamente entre qualidades em redes instáveis.

    Duas fases de operação:

    FASE 1 — Arranque guiado por taxa (buffer < BUFFER_TRANSICAO_S):
        A estimativa de taxa (média dos últimos JANELA_TAXA segmentos, com fator
        de segurança) guia a decisão. Começa em 240p e seleciona a maior qualidade
        cujo bitrate cabe na estimativa atual — mesma lógica do baseline, mas
        preservando _idx_atual para a transição para a Fase 2.
        Sem histórico de taxa, permanece em 240p.

    FASE 2 — Buffer-Based (buffer >= BUFFER_TRANSICAO_S):
        A taxa é ignorada. O nível do buffer e sua tendência guiam a decisão.
        Três zonas de atuação, avaliadas nesta ordem de prioridade:

        1. Subida (buffer >= threshold_subida E tendência positiva por >= 3 seg):
                Sobe 1 degrau; reseta histerese; reativa flag de descida leve.

        2. Descida leve (buffer < BUFFER_DESCE_1_S E flag buffer_desce_1_ON):
                Desce 1 degrau; ativa histerese; desativa flag buffer_desce_1_ON
                até que uma subida ocorra. Evita descidas repetidas na zona
                intermediária sem que o buffer se recupere primeiro.

        3. Descida crítica (buffer < BUFFER_DESCER_S):
                Desce 1 degrau e ativa histerese, independente da flag de descida leve.

        4. Zona segura (demais casos):
                Mantém qualidade e histerese inalteradas.

    FASE 2 com histerese (ativada após qualquer descida):
        O threshold de subida passa de BUFFER_SUBIR_S para
        BUFFER_SUBIR_S + BUFFER_HISTERESE_S, exigindo que o buffer se recupere
        com folga extra antes de tentar subir de novo. Evita o ciclo rápido
        A -> B -> A em redes instáveis. O threshold volta ao normal assim que
        uma subida de qualidade ocorre.

    Mecanismo Auxiliar — Filtro de Tendência (buffer_subir_ON):
    Para autorizar qualquer subida na FASE 2, além de atingir o threshold,
    o buffer deve estar em tendência de crescimento (buffer(n) >= buffer(n-1))
    por pelo menos 3 segmentos consecutivos. Qualquer queda zera o contador.

    Eventos especiais:
    - Rebuffering (buffer zerou): reseta para Fase 1 em 240p, limpa histerese
        e contadores. O histórico de taxa é preservado.

    A transição Fase 1 -> Fase 2 é unidirecional (exceto por rebuffering).
    """

    nome = "BufferBased"

    def __init__(self, representacoes: list):
        self.representacoes    = sorted(representacoes, key=lambda r: r["bitrate_kbps"])
        self._idx_atual        = 0      # começa em 240p
        self._confirmacoes     = 0      # fase 1: confirmações para subir
        self._historico_taxa  = []
        self._modo_buffer      = False  # False = arranque, True = buffer-based
        self._histerese_ativa  = False  # True após uma descida por buffer critico

        self.buffer_desce_1_ON = True  # flag para indicar se o buffer entrou na zona de descida leve

        self._buffer_anterior_s = 0.0
        self._contadorBufferPositivo = 0

    def registrar_taxa(self, taxa_kbps: float):
        self._historico_taxa.append(taxa_kbps)
        if len(self._historico_taxa) > JANELA_TAXA:
            self._historico_taxa.pop(0)

    def taxa_estimada(self) -> float:
        if not self._historico_taxa:
            return 0.0
        return sum(self._historico_taxa) / len(self._historico_taxa)

    @property
    def _threshold_subida(self) -> float:
        """Retorna o threshold de buffer para subir qualidade."""
        if self._histerese_ativa:
            return BUFFER_SUBIR_S + BUFFER_HISTERESE_S   # ex: 7 + 5 = 12s
        return BUFFER_SUBIR_S                             # ex: 7s

    def buffer_subir_ON(self, buffer_nivel_s: float) -> bool:
        """Verifica se o buffer subiu o suficiente para considerar subir a qualidade."""

        if buffer_nivel_s - self._buffer_anterior_s < 0:
            self._contadorBufferPositivo = 0
            #return False
        else:
            self._contadorBufferPositivo += 1
            if self._contadorBufferPositivo >= 3:
                return True
            
        return False
         
        
    def notificar_rebuffer(self):
        """
        Chamado pelo loop principal quando stall_s > 0 (buffer zerou).

        Um rebuffering total significa que o cliente está numa situação
        equivalente ao início da sessão: sem conteúdo disponível, sem
        histórico confiável de buffer. Resetar para o arranque garante
        que a subida de qualidade seja feita de forma segura de novo,
        em vez de tentar retomar do degrau em que estava antes da queda.

        Preserva o histórico de taxa — as medições anteriores ainda
        são úteis para a fase de arranque, que usa taxa para subir.
        """
        self._idx_atual       = 0      # volta para 240p imediatamente
        self._modo_buffer     = False  # retorna à fase de arranque
        self._histerese_ativa = False  # reseta histerese
        self._confirmacoes    = 0
        self._buffer_anterior_s = 0.0
        self._contadorBufferPositivo = 0

    def selecionar_qualidade(self, buffer_nivel_s: float = 0.0) -> dict:
        """Seleciona a qualidade com base no nível do buffer e na fase atual."""
        # verifica transição arranque -> modo buffer
        if not self._modo_buffer and buffer_nivel_s >= BUFFER_TRANSICAO_S:
            self._modo_buffer  = True
            self._confirmacoes = 0

        buffer_subir_on = self.buffer_subir_ON(buffer_nivel_s)
        self._buffer_anterior_s = buffer_nivel_s

        # -------------------------------------------------------
        # FASE 2 — Buffer-Based (com ou sem histerese)
        # -------------------------------------------------------
        if self._modo_buffer:
            if buffer_nivel_s >= self._threshold_subida and buffer_subir_on:
                # buffer atingiu o threshold (normal ou com histerese): sobe
                if self._idx_atual < len(self.representacoes) - 1:
                    self._idx_atual      += 1
                    self._histerese_ativa = False   # subiu: reseta histerese
                self.buffer_desce_1_ON = True   # qualquer subida reativa a descida leve, que só desce 1 degrau


            elif self.buffer_desce_1_ON and buffer_nivel_s < BUFFER_DESCE_1_S : 
                # buffer entre os limiares de descida: desce somente 1 degrau
                if self._idx_atual > 0:
                    self._idx_atual      -= 1
                    self._histerese_ativa = True    # exige folga extra antes de subir
                    self.buffer_desce_1_ON = False   # desativar descida leve até o buffer subir de novo

            elif buffer_nivel_s < BUFFER_DESCER_S:
                # buffer critico: desce e ativa histerese para a próxima subida
                if self._idx_atual > 0:
                    self._idx_atual      -= 1
                    self._histerese_ativa = True    # exige folga extra antes de subir

            # else: zona segura -> mantém qualidade e histerese inalteradas
            
               

            return self.representacoes[self._idx_atual]

        # -------------------------------------------------------
        # FASE 1 — Arranque guiado por taxa
        # -------------------------------------------------------
        if not self._historico_taxa:
            return self.representacoes[self._idx_atual]

        estimativa = self.taxa_estimada() * FATOR_SEGURANCA

        # Percorre do maior para o menor bitrate e pega o primeiro que cabe
        # (mesma lógica da P1, mas preserva _idx_atual para a transição)
        melhor_idx = 0
        for i, rep in enumerate(self.representacoes):   # já ordenado crescente
            if rep["bitrate_kbps"] <= estimativa:
                melhor_idx = i

        self._idx_atual = melhor_idx
        return self.representacoes[self._idx_atual]

# =============================================================================
# Política 3 — xxxxxxx
# =============================================================================

class AbrHybridJitter:
    """
    Política 3 — Híbrida: Buffer-Based (P2) + Thresholds Dinâmicos por Jitter
                e Reação à Tendência do Buffer.

    Herda a estrutura de fases da P2 (arranque -> buffer-based, histerese,
    filtro de tendência de 3 segmentos para autorizar subida), mas substitui
    a lógica de decisão da Fase 2 por uma versão sensível ao jitter e à
    velocidade de variação do buffer (não só ao nível absoluto).

    Componentes adicionados em relação à P2:

    1. EWMA na estimativa de taxa (em vez de média simples):
        taxa_ewma = ALPHA_EWMA * taxa_atual + (1-ALPHA_EWMA) * taxa_ewma_anterior
        Reage mais rápido a quedas de banda que a média simples da P2.
        Usada apenas na Fase 1 (arranque).

    2. Penalidade de jitter (0.0 a 0.5, normalizada por jitter_ewma_ms / 1000):
        penalidade_jitter = min(FATOR_JITTER * jitter_ewma_ms / 1000, 0.5)
        Jitter alto = rede instável = thresholds de buffer mais conservadores.

    3. Thresholds de buffer deslocados dinamicamente pela penalidade de jitter
        (método limites()): quanto maior o jitter, mais cedo a Fase 2 dispara
        descidas e mais tarde libera subidas — o limite de transição arranque
        -> buffer-based (buffer_transicao_s) também se desloca da mesma forma.

    4. Reação à TENDÊNCIA do buffer (variacao_buffer = buffer(n) - buffer(n-1)),
        não só ao nível absoluto:
        - Buffer subindo rápido (>0.65s/seg, ajustado por jitter) -> sobe 1
            degrau mesmo sem atingir o threshold de subida (oportunista).
        - Buffer caindo rápido -> desce preventivamente, mesmo acima do
            threshold crítico; queda muito forte (>1.4s/seg) desce 2 degraus
            de uma vez em vez de 1.

    Comportamento por fase:

    FASE 1 — Arranque (buffer < buffer_transicao_s, que também se desloca
    com o jitter):
        Com buffer crítico (< 2.5s), seleciona pela taxa EWMA com fator de
        segurança, igual à P2. Com buffer > 2.5s e crescendo rápido (>0.75s/seg,
        ajustado por jitter), sobe 1 degrau de forma oportunista.

    FASE 2 — Buffer-Based:
        Quatro gatilhos avaliados a cada segmento, combinando nível absoluto
        do buffer e a tendência (variacao_buffer):
        - Subida normal: buffer >= limite_subida_s E tendência positiva
            por >= 3 segmentos (igual à P2).
        - Subida leve: buffer crescendo rápido, mesmo sem atingir o threshold.
        - Descida crítica: buffer abaixo do limite crítico (deslocado por
            jitter) -> desce 1 degrau, ativa histerese.
        - Descida leve: buffer abaixo do limite leve E caindo, OU queda muito
            acentuada independente do nível -> desce 1 ou 2 degraus conforme
            a intensidade da queda.

    Histerese: igual à P2 — após qualquer descida, o threshold de subida
    fica mais alto até a próxima subida ocorrer.

    Eventos especiais:
    - Rebuffering: reseta para Fase 1 em 240p, limpa histerese e contadores,
        igual à P2 (notificar_rebuffer).
    """

    nome = "HybridJitter"


   

    def __init__(self, representacoes: list):
        self.representacoes  = sorted(representacoes, key=lambda r: r["bitrate_kbps"])
        self._idx_atual      = 0
        self._modo_buffer    = False
        self._histerese_ativa = False
        self.buffer_desce_1_ON = True


        self._buffer_anterior_s      = 0.0
        self._contadorBufferPositivo = 0


        self._taxa_ewma_kbps   = 0.0
        self._jitter_ewma_ms    = 0.0
        self.margem_leve    = 3.0   # segundos de deslocamento máximo na zona leve
        self.margem_critica = 2.0   # segundos de deslocamento máximo na zona crítica
        self._penalidade_jitter = 0.0
        self.limite_descida_leve_s = BUFFER_DESCE_1_S
        self.limite_critico_s = BUFFER_DESCER_S
        self.limite_subida_s = BUFFER_SUBIR_S
        self.buffer_transicao_s = BUFFER_TRANSICAO_S
        self.contadorBufferCaindo = 0


    def registrar_taxa(self, taxa_kbps: float):

        if self._taxa_ewma_kbps == 0.0:
            self._taxa_ewma_kbps = taxa_kbps   # inicializa com a primeira medição
        else:
            self._taxa_ewma_kbps = (
                ALPHA_EWMA * taxa_kbps
                + (1 - ALPHA_EWMA) * self._taxa_ewma_kbps
            )

    def registrar_jitter(self, jitter_EWMA_ms: float):
        self._jitter_ewma_ms = jitter_EWMA_ms   # já vem como EWMA do loop principal
        
    def taxa_estimada(self) -> float:
        return self._taxa_ewma_kbps

    def buffer_subir_ON(self, buffer_nivel_s: float) -> bool:
        if buffer_nivel_s - self._buffer_anterior_s < 0:
            self._contadorBufferPositivo =  0
        else:
            self._contadorBufferPositivo += 1
            if self._contadorBufferPositivo >= 3:
                self._contadorBufferPositivo = 3
                return True
        return False

    def notificar_rebuffer(self):
        self._idx_atual              = 0
        self._modo_buffer            = False
        self._histerese_ativa        = False
        self._buffer_anterior_s      = 0.0
        self._contadorBufferPositivo = 0


    def penalidade_jitter(self) -> float:
        self._penalidade_jitter = min(FATOR_JITTER * (self._jitter_ewma_ms / 1000.0), 0.5)
        return self._penalidade_jitter
    
    def limites(self):

        
        self.limite_descida_leve_s = BUFFER_DESCE_1_S + (self._penalidade_jitter * self.margem_leve / 0.5)
        
        self.limite_critico_s       = BUFFER_DESCER_S  + (self._penalidade_jitter * self.margem_critica / 0.5)

        self.buffer_transicao_s = BUFFER_TRANSICAO_S + (self._penalidade_jitter * self.margem_leve / 0.5)
        
        if self._histerese_ativa:
            self.limite_subida_s = BUFFER_SUBIR_S + BUFFER_HISTERESE_S + (self._penalidade_jitter * self.margem_critica / 0.5)
        else:
            self.limite_subida_s = BUFFER_SUBIR_S + (self._penalidade_jitter * self.margem_critica / 0.5)
    
    
    def selecionar_qualidade(self, buffer_nivel_s: float = 0.0) -> dict:

        buffer_subir_on = self.buffer_subir_ON(buffer_nivel_s)
        variacao_buffer = buffer_nivel_s - self._buffer_anterior_s
        

        if not self._modo_buffer and buffer_nivel_s >= self.buffer_transicao_s:
            self._modo_buffer = True

        # -------------------------------------------------------
        # FASE 2 — Buffer-Based (subida controlada só pelo buffer;
        #          thresholds de descida deslocados por jitter — Opção A)
        # -------------------------------------------------------
        elif self._modo_buffer:

            self.penalidade_jitter()
            self.limites()
            

            # SUBIDA — apenas o buffer decide, igual à P2 original
            if buffer_nivel_s >= self.limite_subida_s and buffer_subir_on:
                print("subindo")
                if self._idx_atual < len(self.representacoes) - 1:
                    self._idx_atual      += 1
                    self._histerese_ativa = False
                self.buffer_desce_1_ON = True

            # SUBIDA LEVE — mesmo sem atingir o threshold de buffer, se o buffer está
            # crescendo rápido (>0.65s por segmento, calibrado empiricamente) a rede
            # está sobrando; sobe 1 degrau de forma oportunista
            if variacao_buffer > 0.65 * (1 + self._penalidade_jitter) and buffer_subir_on:
                print("subindoleve")
                if self._idx_atual < len(self.representacoes) - 1:
                    self._idx_atual      += 1
                self.buffer_desce_1_ON = True

            # DESCIDA CRÍTICA — threshold antecipado por jitter
            elif buffer_nivel_s < self.limite_critico_s :
                print("entrei no critico")
                if self._idx_atual > 0:
                    self._idx_atual      -= 1
                    self._histerese_ativa = True

            # DESCIDA LEVE — dispara em dois cenários (calibrados empiricamente):
            #   (a) buffer abaixo do limite leve E caindo a mais de 0.65s/seg, ou
            #   (b) buffer caindo muito rápido (>1.4s/seg) mesmo fora da zona leve
            # Em ambos, "1.4" indica queda forte -> desce 2 degraus; entre 0.65 e 1.1
            # de queda é considerada queda moderada -> desce só 1 degrau
            elif (buffer_nivel_s < self.limite_descida_leve_s and variacao_buffer < -0.65*(1 - self._penalidade_jitter)) or variacao_buffer < -1.4 *(1 - self._penalidade_jitter) and buffer_nivel_s < self.limite_subida_s:
                if self._idx_atual > 0 :
                    
                    nivel_qualidade = 2
                    if variacao_buffer > -1.1 * (1 + self._penalidade_jitter):
                        nivel_qualidade = 1
                    print("desceleve")
                    self._idx_atual      = max( 0, self._idx_atual - nivel_qualidade)
                    self._histerese_ativa = True




            # else: zona segura -> mantém qualidade e histerese inalteradas
            print(f"  \n\n limites: critc{self.limite_critico_s:.2f} leve{self.limite_descida_leve_s:.2f} subi{self.limite_subida_s:.2f} trans {self.buffer_transicao_s:.2f} variacao {variacao_buffer:.2f} penali {self._penalidade_jitter:.2f} \n\n")
            self._buffer_anterior_s = buffer_nivel_s
            return self.representacoes[self._idx_atual]

        # -------------------------------------------------------
        # FASE 1 — Arranque guiado por taxa penalizada (igual antes)
        # -------------------------------------------------------
        if self._taxa_ewma_kbps == 0.0:
            self._buffer_anterior_s = buffer_nivel_s
            return self.representacoes[self._idx_atual]

        estimativa = self.taxa_estimada() * FATOR_SEGURANCA

        if buffer_nivel_s < 2.5:
            melhor_idx = 0
            for i, rep in enumerate(self.representacoes):
                if rep["bitrate_kbps"] <= estimativa:
                    melhor_idx = i
            self._idx_atual = melhor_idx
        else:
            if variacao_buffer > 0.75 * (1 + self._penalidade_jitter):
                if self._idx_atual < len(self.representacoes) - 1:
                    self._idx_atual += 1

  
        self._buffer_anterior_s = buffer_nivel_s
        return self.representacoes[self._idx_atual]

  
# =============================================================================
# BUFFER MANAGER
# =============================================================================

class BufferManager:
    """
    Gerencia o nível estimado do buffer em segundos.
    """

    def __init__(self, segment_duration_s: float):
        self.segment_duration_s = segment_duration_s
        self.nivel_s            = 0.0
        self._ultimo_tick       = None

    def iniciar_tick(self):
        self._ultimo_tick = time.monotonic()

    def tick(self):
        agora = time.monotonic()
        if self._ultimo_tick is None:
            self._ultimo_tick = agora
            return 0.0

        decorrido         = agora - self._ultimo_tick
        self._ultimo_tick = agora
        stall_s           = 0.0
        self.nivel_s     -= decorrido

        if self.nivel_s < 0:
            stall_s      = -self.nivel_s
            self.nivel_s = 0.0

        return stall_s

    def adicionar_segmento(self):
        self.nivel_s += self.segment_duration_s

    def pode_reproduzir(self) -> bool:
        return self.nivel_s >= BUFFER_MIN_PLAY_S


# =============================================================================
# GRAVAÇÃO DE CSV
# =============================================================================

CAMPOS_CSV = [
    "segment",
    "timestamp",
    "server_id",
    "politica",
    "quality",
    "bitrate_kbps",
    "taxa_kbps",
    "download_time_s",
    "jitter_network_ms",
    "jitter_ewma_ms",
    "buffer_level_s",
    "buffer_can_play",
    "rebuffer_event",
    "stall_duration_s",
    "failover_total",
]

def criar_csv(caminho: str):
    f      = open(caminho, "w", newline="", encoding="utf-8")
    writer = csv.DictWriter(f, fieldnames=CAMPOS_CSV)
    writer.writeheader()
    return writer, f


# =============================================================================
# GERAÇÃO DE GRÁFICOS
# =============================================================================


def gerar_graficos(caminho_csv: str, titulo: str = "TR2 — ABR"):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[aviso] matplotlib não encontrado — gráficos não gerados.")
        return

    segmentos, vazoes, qualidades, buffers, rebuffers, jitters_ewma = [], [], [], [], [], []
    failovers = []
    jitters_network = []  
    mapa_qualidade = {"240p": 1, "360p": 2, "480p": 3, "720p": 4, "1080p": 5}

    ultimo_failover = 0
    with open(caminho_csv, newline="", encoding="utf-8") as f:
        for linha in csv.DictReader(f):
            segmentos.append(int(linha["segment"]))
            vazoes.append(float(linha["taxa_kbps"]))
            qualidades.append(mapa_qualidade.get(linha["quality"], 0))
            buffers.append(float(linha["buffer_level_s"]))
            rebuffers.append(int(linha["rebuffer_event"]))
            jitters_ewma.append(float(linha["jitter_ewma_ms"]))
            jitters_network.append(float(linha["jitter_network_ms"])) 
            ft = int(linha["failover_total"])
            if ft > ultimo_failover:
                failovers.append(int(linha["segment"]))
                ultimo_failover = ft

    labels_qualidade = {1: "240p", 2: "360p", 3: "480p", 4: "720p", 5: "1080p"}

    fig, axs = plt.subplots(4, 1, figsize=(11, 12), sharex=True)
    fig.suptitle(titulo, fontsize=13, fontweight="bold")

    # --- Gráfico 1: taxa ---
    axs[0].plot(segmentos, vazoes, color="steelblue", linewidth=1.5, label="Taxa medida")
    axs[0].set_ylabel("Taxa (kbps)")
    axs[0].legend(loc="upper right", fontsize=8)
    axs[0].grid(True, linestyle="--", alpha=0.5)

    # --- Gráfico 2: qualidade selecionada ---
    axs[1].step(segmentos, qualidades, where="post", color="darkorange", linewidth=1.5)
    axs[1].set_ylabel("Qualidade")
    axs[1].set_yticks(list(labels_qualidade.keys()))
    axs[1].set_yticklabels(list(labels_qualidade.values()))
    axs[1].grid(True, linestyle="--", alpha=0.5)

    # --- Gráfico 3: buffer + rebuffering ---
    axs[2].plot(segmentos, buffers, color="seagreen", linewidth=1.5, label="Buffer (s)")
    axs[2].axhline(BUFFER_MIN_PLAY_S, color="red", linestyle="--",
                   linewidth=1, label=f"Mínimo play ({BUFFER_MIN_PLAY_S}s)")
    for i, rb in enumerate(rebuffers):
        if rb:
            axs[2].axvline(segmentos[i], color="red", alpha=0.4, linewidth=1)
    axs[2].set_ylabel("Buffer (s)")
    axs[2].legend(loc="upper right", fontsize=8)
    axs[2].grid(True, linestyle="--", alpha=0.5)

    # --- Gráfico 4: jitter EWMA vs jitter instantâneo ---
    axs[3].plot(segmentos, jitters_network, color="gold", linewidth=2.0,
                alpha=1.0, label="Jitter instantâneo (ms)")
    axs[3].plot(segmentos, jitters_ewma, color="purple", linewidth=1.5, label="Jitter EWMA (ms)")
    axs[3].set_ylabel("Jitter (ms)")
    axs[3].set_xlabel("Segmento")
    axs[3].legend(loc="upper right", fontsize=8)
    axs[3].grid(True, linestyle="--", alpha=0.5)

    # --- marca eventos de failover em todos os gráficos ---
    for seg_fo in failovers:
        for ax in axs:
            ax.axvline(seg_fo, color="black", linestyle=":", linewidth=1.5,
                       label="Failover" if ax == axs[0] else "")
    if failovers:
        axs[0].legend(loc="upper right", fontsize=8)

    plt.tight_layout()
    caminho_img = caminho_csv.replace(".csv", "_graficos.png")
    plt.savefig(caminho_img, dpi=150)
    print(f"[info] Gráficos salvos em: {caminho_img}")
    plt.close()

def gerar_graficos_comparativo(csv_p1: str, csv_p2: str):
    """Gráficos sobrepostos comparando Política 1 vs Política 2."""
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[aviso] matplotlib não encontrado — gráficos não gerados.")
        return

    def ler_csv(caminho):
        segs, vazoes, quals, bufs, rebufs = [], [], [], [], []
        mapa = {"240p": 1, "360p": 2, "480p": 3, "720p": 4, "1080p": 5}
        with open(caminho, newline="", encoding="utf-8") as f:
            for linha in csv.DictReader(f):
                segs.append(int(linha["segment"]))
                vazoes.append(float(linha["taxa_kbps"]))
                quals.append(mapa.get(linha["quality"], 0))
                bufs.append(float(linha["buffer_level_s"]))
                rebufs.append(int(linha["rebuffer_event"]))
        return segs, vazoes, quals, bufs, rebufs

    s1, v1, q1, b1, r1 = ler_csv(csv_p1)
    s2, v2, q2, b2, r2 = ler_csv(csv_p2)

    labels_qualidade = {1: "240p", 2: "360p", 3: "480p", 4: "720p", 5: "1080p"}

    fig, axs = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    fig.suptitle("TR2 — Comparativo: Política 1 (Rate-Based) vs Política 2 (BufferBased)",
                 fontsize=13, fontweight="bold")

    # --- Taxa ---
    axs[0].plot(s1, v1, color="steelblue",  linewidth=1.5, label="P1 Rate-Based")
    axs[0].plot(s2, v2, color="darkorange", linewidth=1.5, linestyle="--", label="P2 BufferBased")
    axs[0].set_ylabel("Taxa (kbps)")
    axs[0].legend(loc="upper right", fontsize=8)
    axs[0].grid(True, linestyle="--", alpha=0.5)

    # --- Qualidade ---
    axs[1].step(s1, q1, where="post", color="steelblue",  linewidth=1.5, label="P1 Rate-Based")
    axs[1].step(s2, q2, where="post", color="darkorange", linewidth=1.5, linestyle="--", label="P2 BufferBased")
    axs[1].set_ylabel("Qualidade")
    axs[1].set_yticks(list(labels_qualidade.keys()))
    axs[1].set_yticklabels(list(labels_qualidade.values()))
    axs[1].legend(loc="upper right", fontsize=8)
    axs[1].grid(True, linestyle="--", alpha=0.5)

    # --- Buffer ---
    axs[2].plot(s1, b1, color="steelblue",  linewidth=1.5, label="P1 Rate-Based")
    axs[2].plot(s2, b2, color="darkorange", linewidth=1.5, linestyle="--", label="P2 BufferBased")
    axs[2].axhline(BUFFER_MIN_PLAY_S, color="red", linestyle="--",
                   linewidth=1, label=f"Mínimo ({BUFFER_MIN_PLAY_S}s)")
    for i, rb in enumerate(r1):
        if rb:
            axs[2].axvline(s1[i], color="steelblue", alpha=0.3, linewidth=1)
    for i, rb in enumerate(r2):
        if rb:
            axs[2].axvline(s2[i], color="darkorange", alpha=0.3, linewidth=1)
    axs[2].set_ylabel("Buffer (s)")
    axs[2].set_xlabel("Segmento")
    axs[2].legend(loc="upper right", fontsize=8)
    axs[2].grid(True, linestyle="--", alpha=0.5)

    plt.tight_layout()
    base      = csv_p1.replace(".csv", "")
    caminho   = f"{base}_comparativo.png"
    plt.savefig(caminho, dpi=150)
    print(f"[info] Gráfico comparativo salvo em: {caminho}")
    plt.close()


def gerar_graficos_tempo(caminho_csv: str, titulo: str = "TR2 — ABR por Tempo"):
    """
    Gera gráficos com o eixo X em tempo real (segundos desde o início da sessão)
    em vez de número de segmento.
    """
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[aviso] matplotlib não encontrado.")
        return

    tempos, vazoes, qualidades, buffers, rebuffers, jitters_ewma = [], [], [], [], [], []
    failovers = []
    jitters_network = []
    mapa_qualidade = {"240p": 1, "360p": 2, "480p": 3, "720p": 4, "1080p": 5}
    labels_qualidade = {1: "240p", 2: "360p", 3: "480p", 4: "720p", 5: "1080p"}

    t_inicio = None
    ultimo_failover = 0

    with open(caminho_csv, newline="", encoding="utf-8") as f:
        for linha in csv.DictReader(f):
            ts = datetime.fromisoformat(linha["timestamp"])
            if t_inicio is None:
                t_inicio = ts
            t_relativo = (ts - t_inicio).total_seconds()

            tempos.append(t_relativo)
            vazoes.append(float(linha["taxa_kbps"]))
            qualidades.append(mapa_qualidade.get(linha["quality"], 0))
            buffers.append(float(linha["buffer_level_s"]))
            rebuffers.append(int(linha["rebuffer_event"]))
            jitters_ewma.append(float(linha["jitter_ewma_ms"]))
            jitters_network.append(float(linha["jitter_network_ms"]))
            ft = int(linha["failover_total"])
            if ft > ultimo_failover:
                failovers.append(t_relativo)
                ultimo_failover = ft

    fig, axs = plt.subplots(4, 1, figsize=(11, 12), sharex=True)
    fig.suptitle(titulo, fontsize=13, fontweight="bold")

    # --- taxa ---
    axs[0].plot(tempos, vazoes, color="steelblue", linewidth=1.5, label="taxa medida")
    axs[0].set_ylabel("taxa (kbps)")
    axs[0].legend(loc="upper right", fontsize=8)
    axs[0].grid(True, linestyle="--", alpha=0.5)

    # --- Qualidade ---
    axs[1].step(tempos, qualidades, where="post", color="darkorange", linewidth=1.5)
    axs[1].set_ylabel("Qualidade")
    axs[1].set_yticks(list(labels_qualidade.keys()))
    axs[1].set_yticklabels(list(labels_qualidade.values()))
    axs[1].grid(True, linestyle="--", alpha=0.5)

    # --- Buffer ---
    axs[2].plot(tempos, buffers, color="seagreen", linewidth=1.5, label="Buffer (s)")
    axs[2].axhline(2.0, color="red", linestyle="--", linewidth=1, label="Mínimo play (2s)")
    for i, rb in enumerate(rebuffers):
        if rb:
            axs[2].axvline(tempos[i], color="red", alpha=0.4, linewidth=1)
    axs[2].set_ylabel("Buffer (s)")
    axs[2].legend(loc="upper right", fontsize=8)
    axs[2].grid(True, linestyle="--", alpha=0.5)

    # --- Jitter EWMA vs instantâneo ---
    axs[3].plot(tempos, jitters_network, color="gold", linewidth=2.0,
                alpha=1.0, label="Jitter instantâneo (ms)")
    axs[3].plot(tempos, jitters_ewma, color="purple", linewidth=1.5, label="Jitter EWMA (ms)")
    axs[3].set_ylabel("Jitter (ms)")
    axs[3].set_xlabel("Tempo (s)")
    axs[3].legend(loc="upper right", fontsize=8)
    axs[3].grid(True, linestyle="--", alpha=0.5)

    # --- Failovers ---
    for t_fo in failovers:
        for ax in axs:
            ax.axvline(t_fo, color="black", linestyle=":", linewidth=1.5,
                       label="Failover" if ax == axs[0] else "")
    if failovers:
        axs[0].legend(loc="upper right", fontsize=8)

    plt.tight_layout()
    caminho_img = caminho_csv.replace(".csv", "_tempo.png")
    plt.savefig(caminho_img, dpi=150)
    print(f"[info] Gráfico por tempo salvo em: {caminho_img}")
    plt.close()

# =============================================================================
# LOOP PRINCIPAL DE DOWNLOAD
# =============================================================================
def executar_sessao(abr, failover: FailoverManager, buffer: BufferManager,
                    num_segmentos: int, writer):
    """
    Loop central de download de segmentos.
    Garante estritamente que o cliente NUNCA avance de segmento 
    até obter um download válido e com sucesso.
    """
    jitter_ewma_ms = 0.0
    trava_de_falha = True
    tempo_inicio_falha = None
    buffer.tick()
    metricas = []

    for num_seg in range(1, num_segmentos + 1):
        sucesso_download = False

        # O loop só quebra quando o download deste segmento ESPECÍFICO der 100% certo
        while not sucesso_download:
            
            while buffer.nivel_s > BUFFER_MAX_S: # evita baixar se o buffer já estiver cheio 
                time.sleep(0.5)
                buffer.tick()

            representacao   = abr.selecionar_qualidade(buffer.nivel_s)
            buffer_can_play = 1 if buffer.pode_reproduzir() else 0

            # O cabeçalho imprime a cada tentativa, mostrando o nível real do buffer caindo
            buffer.tick()  # atualiza o buffer antes de imprimir o status
            print(f"  seg {num_seg:03d} | qual={representacao['quality']:5s} "
                  f"| buf={buffer.nivel_s:.2f}s "
                  f"| srv={failover.servidor_atual['id']}"
                  f"| taxa_est={abr.taxa_estimada():.0f} kbps ", end="  ")

            # 1. Executa a requisição HTTP
            host_srv, port_srv = failover.url_para_segmento(representacao["url_path"])
            resultado_bruto    = http_get(host_srv, port_srv, representacao["url_path"])
            resultado          = calcular_metricas(resultado_bruto)

            # Considera FALHA: erro de socket, status HTTP inválido ou resposta com 0 bytes
            falhou = bool(resultado["erro"]) or resultado_bruto["status"] not in (200,) or resultado["bytes_totais"] == 0

            # 2. Contabiliza o tempo gasto no buffer (o tempo passa mesmo em caso de falha!)
            stall_s        = buffer.tick()
            rebuffer_event = 1 if stall_s > 0 else 0

            if rebuffer_event and hasattr(abr, 'notificar_rebuffer'):
                abr.notificar_rebuffer()
                print(f'\n  [rebuffer] buffer zerou (stall={stall_s:.2f}s) -> reset para arranque 240p')

            # 3. Análise do Resultado
            if not falhou:
                # === CASO DE SUCESSO ===
                failover.registrar_sucesso()
                print(f"→ {resultado['taxa_kbps']:.0f} kbps  ({resultado['tempo_s']:.2f}s)")

                # Atualiza métricas de rede e algoritmo ABR
                if resultado["taxa_kbps"] > 0:
                    abr.registrar_taxa(resultado["taxa_kbps"])
                    jitter_ewma_ms = (ALPHA_EWMA * resultado["jitter_network_ms"]
                                      + (1 - ALPHA_EWMA) * jitter_ewma_ms)
                    
                if hasattr(abr, 'registrar_jitter'):
                    abr.registrar_jitter(jitter_ewma_ms)

                if resultado["bytes_totais"] > 0:
                    buffer.adicionar_segmento()

                # Grava no CSV apenas o segmento bem-sucedido (1 linha por segmento real no gráfico)
                linha = {
                    "segment":           num_seg,
                    "timestamp":         datetime.now(timezone.utc).isoformat(),
                    "server_id":         failover.servidor_atual["id"],
                    "politica":          abr.nome,
                    "quality":           representacao["quality"],
                    "bitrate_kbps":      representacao["bitrate_kbps"],
                    "taxa_kbps":        round(resultado["taxa_kbps"], 2),
                    "download_time_s":   round(resultado["tempo_s"], 4),
                    "jitter_network_ms": round(resultado["jitter_network_ms"], 3),
                    "jitter_ewma_ms":    round(jitter_ewma_ms, 3),
                    "buffer_level_s":    round(buffer.nivel_s, 3),
                    "buffer_can_play":   buffer_can_play,
                    "rebuffer_event":    rebuffer_event,
                    "stall_duration_s":  round(stall_s, 4),
                    "failover_total":    failover.failover_total,
                }
                writer.writerow(linha)
                metricas.append(linha)

                # PERMISSÃO CONCEDIDA: Sai do loop e avança para o próximo segmento do FOR
                sucesso_download = True
                trava_de_falha = True  # reativa a trava para detectar o início de uma nova falha, se ocorrer
            
            else:
                # === CASO DE FALHA ===
                print(f"[FALHA: status={resultado_bruto['status'] or 0}]", end=" ")
                deve_failover = failover.registrar_falha()

                
                if trava_de_falha:
                    tempo_inicio_falha = time.monotonic()
                    trava_de_falha = False


                if deve_failover:
                    failover_sucesso = failover.tentar_failover()
                    if failover_sucesso:
                        tempo_falha = time.monotonic() - tempo_inicio_falha
                        print(f"\n\n FALHOU POR {tempo_falha:.2f}s. \n\n ")

                        print(f"-> [failover] Migrado para {failover.servidor_atual['id']}. Re-tentando o mesmo segmento...")
                        

                    """ else:
                        # Apocalipse: Ambos servidores caídos!
                        print(f"-> [ALERTA] Ambos fora! Aguardando 1s antes de re-tentar o seg {num_seg:03d}...")
                        time.sleep(1.0) # Pausa estratégica para não travar o computador em loop infinito"""
                else:
                    print(f"\n-> [retry] Tentando novamente no mesmo servidor...")
                    time.sleep(0.5)

                


    return metricas

# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Cliente ABR — TR2 Tarefa 2")
    parser.add_argument("--segmentos", type=int,    default=20,
                        help="Número de segmentos a baixar (padrão: 20)")
    parser.add_argument("--saida",     type=str,    default="sessao.csv",
                        help="Caminho do arquivo CSV de saída (padrão: sessao.csv)")
    parser.add_argument("--politica",  type=int,    default=1, choices=[1, 2, 3],
                        help="Política ABR: 1=Rate-Based (baseline), 2=BufferBased (padrão: 1)")
    parser.add_argument("--comparar",  action="store_true",
                        help="Executa as duas políticas e gera gráfico comparativo")
    parser.add_argument("--proxy", type=int, default=0, choices=[0, 1],
                    help="Redireciona servidores para proxies locais: 1=sim, 0=não (padrão: 0)")
    args = parser.parse_args()

    # --- baixar manifest ---
    print(f"[info] Baixando manifest de {HOST_MANIFEST}:{PORT_MANIFEST} ...")
    resultado_manifest = http_get(HOST_MANIFEST, PORT_MANIFEST, "/manifest")

    if resultado_manifest["erro"] or resultado_manifest["status"] != 200:
        print(f"[erro] Não foi possível baixar o manifest: "
              f"{resultado_manifest['erro'] or resultado_manifest['status']}")
        sys.exit(1)

    manifesto          = json.loads(resultado_manifest["corpo"].decode("utf-8"))
    
    
    """!!! força servidor A e B a passar pelo proxy !!!-------------------------------------------------------"""
    
    if args.proxy == 1:
        manifesto["servers"][0]["url"] = "http://localhost:8080"
        manifesto["servers"][1]["url"] = "http://localhost:8081"
        print("[info] Modo proxy ativo: servidores redirecionados para localhost")

    """!!! força servidor A e B a passar pelo proxy !!!--------------------------------------------------------"""

    segment_duration_s = manifesto["segment_duration_s"]
    representacoes     = manifesto["representations"]
    servidores         = manifesto["servers"]

    print(f"[info] Duração do segmento: {segment_duration_s}s")
    print(f"[info] Qualidades disponíveis: {[r['quality'] for r in representacoes]}")
    print(f"[info] Servidores: {[s['id'] for s in servidores]}")

    # -------------------------------------------------------------------------
    # Modo --comparar: executa P1 e P2 e gera gráfico comparativo
    # -------------------------------------------------------------------------
    if args.comparar:
        base   = args.saida.replace(".csv", "")
        csv_p1 = f"{base}_p1.csv"
        csv_p2 = f"{base}_p2.csv"

        for politica_num, csv_path in [(1, csv_p1), (2, csv_p2)]:
            nome_pol = "Rate-Based (P1)" if politica_num == 1 else "BufferBased (P2)"
            print(f"\n{'='*60}")
            print(f"[info] Executando {nome_pol} → {csv_path}")
            print(f"{'='*60}")

            abr     = AbrRateBased(representacoes) if politica_num == 1 \
                      else AbrBufferBased(representacoes)
            failov  = FailoverManager(servidores)
            buf     = BufferManager(segment_duration_s)
            wrt, f  = criar_csv(csv_path)

            executar_sessao(abr, failov, buf, args.segmentos, wrt)
            f.close()
            print(f"[info] CSV salvo em: {csv_path}")
            titulo = f"TR2 — {nome_pol}"
            gerar_graficos(csv_path, titulo=titulo)

        gerar_graficos_comparativo(csv_p1, csv_p2)
        gerar_graficos_tempo(csv_path, titulo=titulo)   # <-- novo

        return

    # -------------------------------------------------------------------------
    # Modo normal: executa a política escolhida
    # -------------------------------------------------------------------------
    nome_pol = "Rate-Based (P1)" if args.politica == 1 else "BufferBased (P2)" if args.politica == 2 else "HybridJitter (P3)"
    print(f"\n[info] Política: {nome_pol}")
    print(f"[info] Baixando {args.segmentos} segmentos → CSV: {args.saida}\n")

    if args.politica == 1:
        abr = AbrRateBased(representacoes)
    elif args.politica == 2:
        abr = AbrBufferBased(representacoes)
    else:
        abr = AbrHybridJitter(representacoes)

    failov = FailoverManager(servidores)
    buf    = BufferManager(segment_duration_s)
    wrt, f = criar_csv(args.saida)

    executar_sessao(abr, failov, buf, args.segmentos, wrt)
    f.close()

    print(f"\n[info] CSV salvo em: {args.saida}")
    titulo = f"TR2 — {nome_pol}"
    gerar_graficos(args.saida, titulo=titulo)
    gerar_graficos_tempo(args.saida, titulo=titulo)  

if __name__ == "__main__":
    main()
