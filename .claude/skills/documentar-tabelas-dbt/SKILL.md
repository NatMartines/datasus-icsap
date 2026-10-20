---
name: documentar-tabelas-dbt
description: Gera a documentação de modelos dbt em português (descrições no schema.yml, testes sugeridos e dicionário de dados) a partir do SQL dos modelos e das colunas.
---

# Documentar tabelas dbt

Use quando a pessoa pedir para documentar modelos dbt, preencher descrições no schema.yml, sugerir testes ou montar um dicionário de dados.

## Entradas

Peça (ou leia, se já estiverem na conversa ou nos arquivos):
1. O SQL de cada modelo (`models/**/*.sql`).
2. O `schema.yml` atual da pasta, se existir (para não apagar o que já está lá).
3. Opcional: uma amostra de linhas ou a saída de `select ... limit 10`, para entender os valores.
4. Opcional: o dicionário da fonte original (ex.: DATASUS, IBGE), para traduzir códigos.

Não invente o significado de uma coluna. Se não der para deduzir pelo SQL, pela amostra ou pela fonte, marque a descrição com `[confirmar]` e liste a dúvida no final.

## Passos

1. **Entender o modelo**: camada (staging, intermediate, marts), grão (o que é uma linha), de onde vem (`ref`/`source`) e filtros aplicados.
2. **Descrever o modelo** em uma ou duas frases: o que contém, o grão e a regra de negócio principal.
3. **Descrever cada coluna** em uma frase curta, em português:
   - o que significa para o negócio;
   - unidade (R$, dias, anos) quando houver;
   - significado dos códigos (ex.: `1 = masculino, 3 = feminino`);
   - de qual coluna da fonte ela veio, quando o nome mudou.
4. **Sugerir testes** com a sintaxe atual do dbt (`arguments:` dentro do teste):
   - chaves: `unique` + `not_null`;
   - códigos com lista fechada: `accepted_values` (use `quote: false` para números);
   - chaves estrangeiras da fato: `relationships` para a dimensão; use `config: severity: warn` quando houver falhas conhecidas da fonte e explique o motivo na descrição;
   - medidas obrigatórias: `not_null`.
5. **Mesclar** com o `schema.yml` existente: mantenha testes e descrições que já estão lá, acrescente o que falta e não mude a indentação (2 espaços).
6. **Entregar**:
   - o bloco YAML pronto para colar, um por arquivo `schema.yml`;
   - se pedido, um dicionário de dados em tabela (modelo, coluna, tipo, descrição, origem);
   - a lista de itens `[confirmar]`.

## Estilo

- Português claro, frases curtas, sem jargão desnecessário.
- Uma descrição por coluna, sem repetir o nome da coluna.
- Explique decisões de negócio na descrição do modelo (ex.: "apenas AIH tipo 1").

## Exemplo de saída

```yaml
version: 2

models:
  - name: fct_internacoes
    description: Uma linha por internação (AIH tipo 1). Fato central do star schema.
    columns:
      - name: num_aih
        description: Número da AIH, identificador único da internação.
        tests: [unique, not_null]
      - name: valor_total
        description: Valor total pago pelo SUS pela internação, em R$.
        tests: [not_null]
      - name: sexo
        description: 1 = masculino, 3 = feminino.
        tests:
          - accepted_values:
              arguments:
                values: ['1', '3']
```
