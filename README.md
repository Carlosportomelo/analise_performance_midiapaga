<<<<<<< HEAD
# Análise de Performance de Mídia Paga

Este projeto contém scripts para análise de performance de mídia paga, integrando dados do Meta Ads e HubSpot.

## Estrutura do Projeto

```
.
├── data/                    # Dados brutos e processados
│   ├── meta_dataset.csv     # Base de dados do Meta
│   └── hubspot_dataset.csv  # Base de dados do HubSpot
├── outputs/                 # Arquivos de saída
│   └── Hubspot_Meta_Base.xlsx  # Base consolidada HubSpot + Meta
└── scripts/                 # Scripts de processamento
    ├── analise_performance_meta.py     # Processamento dados Meta
    ├── analise_performance_hubspot.py  # Processamento dados HubSpot
    └── validar_investimentos.py        # Validação de consistência
```

## Fluxo de Atualização

### 1. Atualização Meta Ads
1. Exportar dados do Meta Ads para `data/meta_dataset.csv`
2. Executar o script de processamento Meta:
   ```powershell
   python scripts/analise_performance_meta.py
   ```

### 2. Atualização HubSpot
1. Exportar dados do HubSpot para `data/hubspot_dataset.csv`
2. Executar o script de processamento HubSpot:
   ```powershell
   python scripts/analise_performance_hubspot.py
   ```

### 3. Validação
Após atualizar ambas as bases, executar o script de validação:
```powershell
python scripts/validar_investimentos.py
```

## Metodologia

### Investimentos
- A base oficial de investimentos vem do Meta Ads
- O script do HubSpot utiliza os mesmos valores de investimento do Meta
- O investimento é prorrateado no HubSpot com base nos negócios realizados

### Prorrateamento
- O valor de investimento diário é distribuído proporcionalmente ao número de negócios
- Se não houver negócios no dia, o investimento é distribuído igualmente entre as campanhas

## Scripts

### analise_performance_meta.py
- Processa os dados brutos do Meta Ads
- Gera métricas básicas (CPL, CTR, etc.)
- Consolida investimentos diários

### analise_performance_hubspot.py
- Integra dados do HubSpot com investimentos do Meta
- Calcula métricas de negócio (custo por negócio, etc.)
- Realiza o prorrateamento do investimento

### validar_investimentos.py
- Verifica consistência dos valores entre Meta e HubSpot
- Compara investimentos mensais
- Alerta sobre possíveis discrepâncias

## Manutenção

### Arquivos a Manter
1. analise_performance_meta.py
2. analise_performance_hubspot.py
3. validar_investimentos.py

### Arquivos a Remover
- Todas as versões anteriores (v1, v2, v3, etc.)
- Scripts de teste ou desenvolvimento

## Looker Studio

### Bases para Dashboard
1. Meta Dashboard:
   - Fonte: `meta_dataset.csv`
   - Métricas: Leads, CPL, Investimento

2. HubSpot Dashboard:
   - Fonte: `outputs/Hubspot_Meta_Base.xlsx`
   - Métricas: Negócios, Matrículas, Custo por Negócio

### Atualizações
1. Sempre atualizar o Meta primeiro
2. Em seguida, processar HubSpot
3. Validar consistência
4. Atualizar fontes no Looker Studio
=======
# analise_performance_midiapaga
Pipeline de Análise de Performance de Mídia Paga: Integra Meta Ads, Google Ads e HubSpot CRM. O sistema limpa dados, normaliza funil de vendas, atribui investimento por lead (prorrateio) e gera relatórios granulares e agregados (.xlsx) para análise de BI de RVO e Matrículas por Ciclo de Captação.
>>>>>>> 61df75b6716e47cc3c36a71272a99f488e710b36
