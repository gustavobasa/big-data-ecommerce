\# Big Data E-commerce

\#Grupo:
Gustavo Barros Sampaio - 2021020120;
Daniel Cláudio Rancan e Silva - 2021020043;
Gabriel da Silva Braga - 2023020063;
Marcelino



\## 1. Visão geral



Este projeto implementa um pipeline de Big Data para processamento de

eventos de um sistema de e-commerce.



O projeto integra diferentes tecnologias para realizar ingestão,

processamento em streaming, processamento analítico, armazenamento e

consulta dos resultados.



As principais tecnologias utilizadas são:



\- Apache Flume

\- HDFS

\- Apache Flink

\- Apache Spark

\- Apache HBase

\- Apache Hive

\- Docker Compose



O projeto foi desenvolvido de forma incremental durante as semanas da

atividade, chegando na Semana 4 à integração dos componentes em um

pipeline ponta a ponta.



\---



\## 2. Arquitetura



O fluxo geral do projeto é:



```text

                   Eventos de e-commerce

                            |

                            v

                          Flume
                            |

                            v

                      flume-output/

                        /         \\

                       /           \\

                      v             v

                   Flink          Spark

                      |              |

                      |              +------> Parquet

                      |                         |

                      |                         v

                      |                        Hive

                      |

                      v

                    HBase

                      ^

                      |

                    Spark


O Flume realiza a ingestão dos eventos e grava os dados em arquivos.



O Flink utiliza esses eventos para processamento em streaming e geração

de alertas.



O Spark utiliza os dados para processamento ETL e geração de insights

de negócio.



Os resultados analíticos do Spark são gravados em formato Parquet e

disponibilizados para consulta através do Hive.



O HBase armazena os insights produzidos pelo Spark e os alertas

produzidos pelo Flink.



\## 3. Tecnologias utilizadas

Tecnologia	Função

Apache Flume	Captura e ingestão dos eventos

HDFS	Armazenamento distribuído

Apache Flink	Processamento dos eventos em streaming

Apache Spark	Processamento ETL e geração de insights

Apache HBase	Armazenamento dos insights e alertas

Apache Hive	Consulta dos dados armazenados em Parquet

Docker Compose	Orquestração dos serviços

\## 4. Ingestão dos eventos com Flume



O Apache Flume é utilizado para realizar a captura dos eventos gerados

pelo sistema de e-commerce.



Os eventos são encaminhados para o diretório:



flume-output/



Esse diretório serve como fonte de dados para os componentes de

processamento.



Os mesmos eventos capturados pelo Flume podem ser utilizados pelo

processamento do Flink e pelo processamento do Spark.



\## 5. Processamento com Apache Flink



O processamento em streaming é realizado pelo job:



flink-job/src/main/java/com/ecommerce/flink/EcommerceStreamJob.java



O job realiza a leitura contínua dos eventos produzidos pelo Flume.



Os eventos são processados utilizando janelas baseadas no tempo dos

eventos e agrupados por produto.



O job calcula a quantidade de eventos de cada produto dentro da janela.



Quando uma janela possui pelo menos 5 eventos para determinado produto,

é gerado um alerta de produto em alta.



Os alertas são enviados para o HBase.



A tabela utilizada para os alertas é:



flink\_alertas

\## 6. Processamento com Apache Spark



O processamento analítico é realizado pelo job:



spark-job/etl\_insights.py



O job utiliza os eventos produzidos pelo Flume e realiza operações de

processamento e transformação dos dados.



Entre as operações utilizadas estão:



deduplicação;

JOIN;

agregações;

GROUP BY.



Essas operações podem provocar operações de shuffle no Spark e

representam dependências largas (wide dependencies) no processamento.



\## 7. Insights de negócio



O processamento Spark calcula quatro insights principais:



7.1 Faturamento por categoria de produto



Calcula o faturamento agrupado por categoria de produto.



7.2 Faturamento por cidade e estado



Calcula o faturamento agrupado por localização.



7.3 Taxa de abandono de carrinho



Calcula a taxa relacionada aos eventos de abandono de carrinho.



7.4 Distribuição de status de entrega



Calcula a distribuição dos diferentes status relacionados às entregas.



Os resultados do processamento são armazenados em formato Parquet e

também enviados para o HBase.



\## 8. HBase



O HBase é utilizado como armazenamento dos resultados gerados pelos

processamentos Spark e Flink.



As principais tabelas utilizadas são:



insights\_categoria

insights\_cidade

flink\_alertas



Os insights gerados pelo Spark são armazenados nas tabelas de insights.



Os alertas gerados pelo Flink são armazenados na tabela

flink\_alertas.



O HBase também disponibiliza uma API REST utilizada pelos jobs para

realizar a comunicação com o banco.



9\. Decisão técnica: HBase via API REST



Foi utilizada a API REST do HBase para a comunicação dos jobs Spark e

Flink com o banco.



Essa decisão evita a necessidade de adicionar e compatibilizar diferentes

versões dos client jars do HBase nos ambientes Spark e Flink.



Dessa forma, os componentes utilizam comunicação HTTP/JSON para enviar

os resultados ao HBase.



No Flink, a comunicação é realizada utilizando o HttpClient disponível

no Java.



No Spark, a comunicação é realizada através do código auxiliar:



spark-job/hbase\_rest.py

\## 10. Parquet e Hive



Os resultados analíticos produzidos pelo Spark são gravados em formato

Parquet.



Esses arquivos são utilizados pelo Hive através de tabelas externas.



Dessa forma, o Spark realiza o processamento e persistência dos

resultados, enquanto o Hive permite consultar os dados através de SQL.



\## 11. HDFS



O projeto também possui um ambiente HDFS composto por:



NameNode;

DataNode.



O NameNode disponibiliza seu painel web na porta:



9870



O HDFS faz parte da arquitetura do ambiente de Big Data utilizado no

projeto.



\## 12. Docker Compose



Os componentes do projeto são executados utilizando Docker Compose.



Para iniciar o ambiente, execute na raiz do projeto:



docker compose up -d



Para verificar os containers:



docker compose ps



Para visualizar os logs de um serviço:



docker compose logs -f NOME\_DO\_SERVICO



Para parar o ambiente:



docker compose down

\## 13. Serviços e portas

Serviço	Porta

HDFS NameNode	9870

Flink JobManager	8081

Spark Master	8080

Spark Worker	8082

HBase REST	8083

HBase Master	16010

HiveServer2	10000

\## 14. Estrutura do projeto

big-data-ecommerce/

│

├── data/

├── flume/

├── flume-output/

├── flume-state/

│

├── flink-job/

│   ├── pom.xml

│   ├── src/

│   └── target/

│

├── flink-output/

│

├── spark-job/

│   ├── README.md

│   ├── etl\_insights.py

│   └── hbase\_rest.py

│

├── spark-output/

├── hbase-data/

├── hive-conf/

│

├── docker-compose.yml

├── hadoop.env

├── events.log

├── .gitignore

└── README.md



Os diretórios e arquivos gerados durante a execução podem não estar

versionados no Git, conforme definido no .gitignore.



\## 15. Integração ponta a ponta



A integração final do projeto utiliza o seguinte fluxo:



Eventos de e-commerce

         |

         v

       Flume

         |

         v

   flume-output/

      /       \\

     /         \\

    v           v

 Flink        Spark

    |           |

    |           +--------> Parquet

    |                       |

    |                       v

    |                      Hive

    |

    +--------------------> HBase

                             ^

                             |

                           Spark



O Flume realiza a ingestão dos eventos.



O Flink processa os eventos em streaming e gera alertas quando a

quantidade de eventos de um produto dentro de uma janela atinge o

limiar configurado.



O Spark processa os dados para gerar os insights de negócio.



O HBase recebe os resultados dos processamentos.



O Hive disponibiliza os dados Parquet para consulta.



\## 16. Dependências largas no Spark



O job Spark utiliza operações que provocam movimentação dos dados entre

partições.



Entre essas operações estão:



deduplicação;

JOIN;

GROUP BY;

agregações.



Essas operações são utilizadas para demonstrar o conceito de

dependências largas (wide dependencies) e operações de shuffle no Spark.



\## 17. Organização das responsabilidades



Cada tecnologia possui uma responsabilidade específica dentro do

pipeline:



Flume

&#x20; -> ingestão dos eventos



HDFS

&#x20; -> armazenamento distribuído



Flink

&#x20; -> processamento em streaming e alertas



Spark

&#x20; -> processamento ETL e geração dos insights



HBase

&#x20; -> armazenamento dos insights e alertas



Hive

&#x20; -> consulta dos resultados em Parquet



Docker Compose

&#x20; -> orquestração do ambiente

\## 18. Semana 4 - Integração e entrega



A Semana 4 tem como objetivo integrar os componentes desenvolvidos nas

etapas anteriores e demonstrar o funcionamento do pipeline de ponta a

ponta.



A entrega contempla:



integração dos componentes;

pipeline de processamento;

geração dos insights;

geração dos alertas;

armazenamento dos resultados;

documentação das decisões técnicas;

repositório GitHub finalizado;

vídeo de apresentação com duração máxima de 5 minutos.
