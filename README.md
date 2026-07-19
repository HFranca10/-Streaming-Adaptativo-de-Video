# Streaming Adaptativo com Controle de Banda (ABR sobre HTTP)
> **Disciplina:** Teleinformática e Redes 2 (TR2) — Projeto Final

Este repositório contém a implementação do **Cliente de Streaming Adaptativo** desenvolvido em Python puro para a disciplina de TR2. O sistema é capaz de consumir mídias via variantes do protocolo DASH (Dynamic Adaptive Streaming over HTTP), aplicando algoritmos de ABR (Adaptive Bitrate), gerenciamento dinâmico de buffer e failover automático entre servidores.

---

## 🚀 Arquitetura do Sistema

O projeto interage com a infraestrutura fornecida pela disciplina, composta por dois servidores HTTP com controle de banda e jitter programáticos:
* **Servidor A (Principal):** `http://137.131.178.229:8080`
* **Servidor B (Fallback):** `http://137.131.178.229:8081`

O cliente desenvolvido realiza o parser do `manifest.json`, calcula métricas de rede por segmento e toma decisões em tempo real para mitigar *stalls* (rebuffering).

---

## 🛠️ Algoritmos ABR Implementados

O projeto avalia três políticas distintas de adaptação de qualidade:
1. **Política 1 — Baseline (Rate-Based):** Seleciona a maior qualidade baseando-se puramente na vazão média recente com um fator de segurança.
2. **Política 2 — [Nome da sua Política 2, ex: Buffer-Based / Histerese]:** Desenvolvida para corrigir as oscilações e deficiências numéricas identificadas no Baseline.
3. **Política 3 — [Nome da sua Política 3, ex: EWMA / Híbrida]:** Abordagem estatística/heurística avançada projetada para tratar cenários com alto *jitter* de rede.

---

## 📈 Métricas Coletadas (Mapeamento do CSV)

A cada segmento baixado, o cliente exporta os seguintes dados estruturados para análise estatística e correlação com o Wireshark:
* `segment`, `timestamp`, `server_id`, `quality`, `bitrate_kbps`
* `vazão_kbps`, `download_time_s`, `variação de atraso (jitter)_network_ms`, `variação de atraso (jitter)_ewma_ms`
* `buffer_level_s`, `buffer_can_play`, `rebuffer_event`, `stall_duration_s`, `failover_total`

---

## 📦 Como Instalar e Executar

### Pré-requisitos
* Python 3.6 ou superior
* Biblioteca para geração de gráficos (instalação para análise):
  ```bash
  pip install matplotlib
