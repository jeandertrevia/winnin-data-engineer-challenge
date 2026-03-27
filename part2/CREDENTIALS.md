# Credenciais do ambiente local

## Airflow

| Campo    | Valor                        |
|----------|------------------------------|
| URL      | http://localhost:8080        |
| Usuario  | admin                        |
| Senha    | admin                        |

## Postgres

| Campo    | Valor        |
|----------|--------------|
| Host     | localhost    |
| Porta    | 5432         |
| Usuario  | airflow      |
| Senha    | airflow      |

### Bancos de dados

| Banco       | Uso                          |
|-------------|------------------------------|
| airflow     | Metadados internos do Airflow |
| creators_db | Dados dos creators (raw + dbt) |

## Conexao via cliente SQL (ex: DBeaver, TablePlus)

```
Host:     localhost
Port:     5432
Database: creators_db
User:     airflow
Password: airflow
```
