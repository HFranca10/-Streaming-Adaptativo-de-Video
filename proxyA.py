"""

python proxy.py --queda 10
python proxy.py --queda 10 --jitter-min 0 --jitter-max 50
python proxy.py --queda 10 --jitter-min 50 --jitter-max 300

proxy_simulador.py — Simula a queda do servidor A após N segmentos bem-sucedidos
                      e/ou injeta jitter artificial entre chunks da resposta.

Como funciona:
  - Escuta numa porta local (padrão: 8080)
  - Encaminha todas as requisições para o servidor real (HOST_REAL:PORTA_REAL)
  - Conta quantos segmentos de vídeo foram baixados com sucesso
  - Após SEGMENTOS_ANTES_DA_QUEDA segmentos, começa a recusar conexões (simula queda)
  - Após TEMPO_RECOVERY_S segundos caído, volta a aceitar conexões normalmente
  - Health checks (/health) sempre são respondidos, mesmo com o proxy "caído",
    para permitir que o FailoverManager do cliente detecte a recuperação
  - Opcionalmente, atrasa cada chunk retransmitido por um valor aleatório
    entre --jitter-min e --jitter-max (em ms), simulando uma rede instável

Uso:
  python proxy_simulador.py                        # padrão: porta 8080, cai após 10 segmentos
  python proxy_simulador.py --porta 9090           # porta local diferente
  python proxy_simulador.py --queda 5              # cai após 5 segmentos
  python proxy_simulador.py --real-host 1.2.3.4    # servidor real diferente
  python proxy_simulador.py --real-porta 8081      # porta do servidor real diferente
  python proxy_simulador.py --jitter-min 0 --jitter-max 50    # jitter leve
  python proxy_simulador.py --jitter-min 50 --jitter-max 300  # jitter forte

No manifest do cliente, aponte o servidor A para:
  "url": "http://localhost:8080"   (ou a porta que você escolheu)
"""

import socket
import threading
import argparse
import time
import sys
import random


# =============================================================================
# CONFIGURAÇÕES PADRÃO
# =============================================================================

HOST_REAL_PADRAO     = "137.131.178.229"
PORTA_REAL_PADRAO    = 8080
PORTA_LOCAL_PADRAO   = 8080
QUEDA_PADRAO         = 10          # segmentos antes de "cair"
BUFFER_SIZE          = 65536       # 64 KB por leitura de socket
TEMPO_RECOVERY_PADRAO = 5.0       # segundos até o servidor "voltar" sozinho

JITTER_MIN_MS_PADRAO = 0.0         # delay mínimo por chunk (ms)
JITTER_MAX_MS_PADRAO = 0.0         # delay máximo por chunk (ms) — 0 desativa o jitter


# =============================================================================
# ESTADO GLOBAL
# =============================================================================

class Estado:
    def __init__(self, queda_apos: int, tempo_recovery_s: float):
        self._lock             = threading.Lock()
        self._segmentos        = 0
        self._queda_apos       = queda_apos
        self._caiu             = False
        self._total_requests   = 0

        self._tempo_queda      = None              # registra quando caiu
        self.TEMPO_RECOVERY_S  = tempo_recovery_s   # tempo até voltar

    @property
    def caiu(self) -> bool:
        return self._caiu

    def registrar_requisicao(self, path: str) -> bool:
        """
        Registra uma requisição recebida.
        Retorna True se deve responder normalmente, False se deve recusar.
        """
        with self._lock:
            self._total_requests += 1

            if self._caiu:
                # health check sempre passa — permite que o failover detecte a recuperação
                if path == "/health":
                    return True

                if time.time() - self._tempo_queda >= self.TEMPO_RECOVERY_S:
                    self._caiu        = False
                    self._tempo_queda = None
                    print(f"\n{'='*60}")
                    print(f"  [proxy] SERVIDOR VOLTOU após {self.TEMPO_RECOVERY_S}s de inatividade!")
                    print(f"  [proxy] Requisições voltarão a ser encaminhadas normalmente.")
                    print(f"{'='*60}\n")
                else:
                    return False  # ainda offline

            # conta apenas segmentos de vídeo (não manifest, não health check)
            eh_segmento = (
                path != "/manifest"
                and path != "/health"
                and not path.endswith(".json")
            )

            if eh_segmento:
                self._segmentos += 1
                print(f"  [proxy] segmento #{self._segmentos} entregue  ({path})")

                if self._segmentos >= self._queda_apos:
                    self._caiu        = True
                    self._tempo_queda = time.time()
                    self._segmentos   = 0
                    print(f"\n{'='*60}")
                    print(f"  [proxy] SERVIDOR DERRUBADO! Voltará em {self.TEMPO_RECOVERY_S}s.")
                    print(f"{'='*60}\n")

            return True  # ainda vivo


# =============================================================================
# HANDLER DE CONEXÃO
# =============================================================================

def handle_client(conn_cliente: socket.socket, addr, estado: Estado,
                  host_real: str, porta_real: int,
                  jitter_min_ms: float, jitter_max_ms: float):
    try:
        # lê a requisição completa do cliente (até \r\n\r\n)
        dados = b""
        conn_cliente.settimeout(10)
        while b"\r\n\r\n" not in dados:
            chunk = conn_cliente.recv(4096)
            if not chunk:
                break
            dados += chunk

        if not dados:
            conn_cliente.close()
            return

        # extrai o path da primeira linha (GET /path HTTP/1.x)
        primeira_linha = dados.split(b"\r\n")[0].decode("utf-8", errors="ignore")
        partes = primeira_linha.split(" ")
        path   = partes[1] if len(partes) >= 2 else "/"

        # decide se responde ou recusa
        if not estado.registrar_requisicao(path):
            # simula queda: fecha conexão sem responder (connection refused / timeout)
            print(f"  [proxy] recusando {addr[0]} → {path}")
            conn_cliente.close()
            return

        # encaminha para o servidor real
        try:
            conn_real = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            conn_real.settimeout(10)
            conn_real.connect((host_real, porta_real))

            # reescreve o Host header para o servidor real
            requisicao_reescrita = dados.replace(
                f"Host: localhost".encode(),
                f"Host: {host_real}:{porta_real}".encode(),
            )
            # também cobre variantes com porta explícita no Host original
            for porta_local in [8080, 8081, 9090, 9091]:
                requisicao_reescrita = requisicao_reescrita.replace(
                    f"Host: localhost:{porta_local}".encode(),
                    f"Host: {host_real}:{porta_real}".encode(),
                )

            conn_real.sendall(requisicao_reescrita)

            # retransmite a resposta de volta ao cliente, com jitter artificial opcional
            while True:
                chunk = conn_real.recv(BUFFER_SIZE)
                if not chunk:
                    break

                # --- injeta jitter artificial entre chunks ---
                if jitter_max_ms > 0:
                    delay_s = random.uniform(jitter_min_ms, jitter_max_ms) / 1000.0
                    time.sleep(delay_s)
                # ----------------------------------------------

                conn_cliente.sendall(chunk)

            conn_real.close()

        except Exception as e:
            print(f"  [proxy] erro ao conectar no servidor real: {e}")
            # responde 502 ao cliente para não deixá-lo pendurado
            resp_erro = (
                b"HTTP/1.0 502 Bad Gateway\r\n"
                b"Content-Length: 0\r\n"
                b"Connection: close\r\n"
                b"\r\n"
            )
            try:
                conn_cliente.sendall(resp_erro)
            except Exception:
                pass

    except Exception as e:
        print(f"  [proxy] erro no handler: {e}")
    finally:
        try:
            conn_cliente.close()
        except Exception:
            pass


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Proxy simulador de queda + jitter — TR2")
    parser.add_argument("--porta",      type=int, default=PORTA_LOCAL_PADRAO,
                        help=f"Porta local de escuta (padrão: {PORTA_LOCAL_PADRAO})")
    parser.add_argument("--real-host",  type=str, default=HOST_REAL_PADRAO,
                        help=f"Host do servidor real (padrão: {HOST_REAL_PADRAO})")
    parser.add_argument("--real-porta", type=int, default=PORTA_REAL_PADRAO,
                        help=f"Porta do servidor real (padrão: {PORTA_REAL_PADRAO})")
    parser.add_argument("--queda",      type=int, default=QUEDA_PADRAO,
                        help=f"Segmentos antes da queda simulada (padrão: {QUEDA_PADRAO})")
    parser.add_argument("--recovery",   type=float, default=TEMPO_RECOVERY_PADRAO,
                        help=f"Segundos até o servidor voltar sozinho após a queda (padrão: {TEMPO_RECOVERY_PADRAO})")
    parser.add_argument("--jitter-min", type=float, default=JITTER_MIN_MS_PADRAO,
                        help=f"Delay mínimo por chunk em ms (padrão: {JITTER_MIN_MS_PADRAO})")
    parser.add_argument("--jitter-max", type=float, default=JITTER_MAX_MS_PADRAO,
                        help=f"Delay máximo por chunk em ms (padrão: {JITTER_MAX_MS_PADRAO}, 0 = desativado)")
    args = parser.parse_args()

    if args.jitter_max < args.jitter_min:
        print("[erro] --jitter-max não pode ser menor que --jitter-min")
        sys.exit(1)

    estado = Estado(queda_apos=args.queda, tempo_recovery_s=args.recovery)

    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", args.porta))
    servidor.listen(50)

    jitter_status = (
        f"{args.jitter_min:.0f}–{args.jitter_max:.0f} ms por chunk"
        if args.jitter_max > 0 else "desativado"
    )

    print(f"╔══════════════════════════════════════════════════════╗")
    print(f"║         proxy_simulador.py — TR2 Failover Test       ║")
    print(f"╠══════════════════════════════════════════════════════╣")
    print(f"║  Escutando em        : localhost:{args.porta:<26}║")
    print(f"║  Servidor real       : {args.real_host}:{args.real_porta:<20}║")
    print(f"║  Queda após          : {args.queda} segmentos{' '*22}║")
    print(f"║  Recovery automático  : {args.recovery:.0f}s{' '*29}║")
    print(f"║  Jitter artificial   : {jitter_status:<32}║")
    print(f"╚══════════════════════════════════════════════════════╝")
    print(f"\nAponte o servidor do cliente para: http://localhost:{args.porta}\n")

    try:
        while True:
            conn, addr = servidor.accept()
            t = threading.Thread(
                target=handle_client,
                args=(conn, addr, estado, args.real_host, args.real_porta,
                      args.jitter_min, args.jitter_max),
                daemon=True,
            )
            t.start()
    except KeyboardInterrupt:
        print(f"\n[proxy] Encerrado. Total de requisições recebidas: {estado._total_requests}")
        servidor.close()
        sys.exit(0)


if __name__ == "__main__":
    main()
