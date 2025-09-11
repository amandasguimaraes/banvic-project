# Banvic Pipeline

Este repositório contém a solução desenvolvida para o desafio de simular um projeto de Engenharia de Dados para o Banco Vitória (BanVic). O principal objetivo é criar um pipeline de ETL para centralizar dados de diferentes fontes em um Data Warehouse local.

O projeto utiliza Apache Airflow para gerenciar e automatizar o fluxo de trabalho. A DAG foi projetada para extrair dados de um arquivo CSV e de um banco de dados SQL, consolidando-os de forma eficiente e idempotente.

### Visão Geral

O pipeline segue um fluxo de trabalho em três etapas:

1.  **Extração Paralela:** Extrai dados de duas fontes (um arquivo CSV e um banco de dados SQL) de forma simultânea. As extrações são idempotentes, garantindo que o resultado seja o mesmo, mesmo se a tarefa for executada múltiplas vezes.
2.  **Verificação:** Uma etapa de verificação de sucesso que garante que ambas as extrações foram concluídas com êxito.
3.  **Carregamento:** Carrega os dados extraídos no Data Warehouse (PostgreSQL) somente após a conclusão bem-sucedida das etapas de extração.

### Requisitos e Configuração

Para rodar este projeto, você precisa ter o Docker e o Docker Compose instalados em sua máquina.

### Execução do Projeto

Siga os passos abaixo para iniciar o ambiente do Airflow e rodar o pipeline.

1.  **Navegue até o diretório do projeto:**
    ```bash
    cd banvic-project/
    ```

2.  **Inicie o ambiente Astro Dev:**
    Este comando irá construir e iniciar os serviços do Airflow, incluindo o webserver, o scheduler e o banco de dados.
    ```bash
    astro dev start
    ```

3.  **Acesse a Interface do Airflow:**
    Após o ambiente subir, acesse a interface do usuário do Airflow no seu navegador:
    [http://localhost:8080](http://localhost:8080)

4.  **Execute a DAG:**
    * Encontre a DAG chamada `banvic_pipeline` na lista.
    * Ative o botão para "unpause" a DAG.
    * Você pode aguardar o horário agendado (04:35) ou disparar a execução manual clicando no ícone de "play" no menu da DAG.

### Evolução Futura

A solução apresentada é a versão 01, focada em atender aos requisitos. Para um ambiente de produção real, a versão 02 seria a próxima etapa, com os seguintes aprimoramentos:

-   **Extração Containerizada:** Utilizar o `DockerOperator` do Airflow para rodar as extrações em contêineres separados. Isso isolaria as dependências de cada extração e evitaria a sobrecarga do scheduler do Airflow.
-   **Melhoria na Ingestão de Dados:** Substituir o Pandas, que é ideal para volumes menores, por uma ferramenta de ingestão escalável como o Meltano ou um framework de extração otimizado para big data.
