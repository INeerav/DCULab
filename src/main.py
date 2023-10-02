import sys
from pathlib import Path



# Do this only for PROD runs
if settings.ETL_STAGE == 0:
    init_logger(
        settings.LOG_GROUP,
        settings.REGION,
        settings.ENVIRONMENT,
        settings.AWS_ENDPOINT_URL,
    )

log = structlog.get_logger()


def get_postgres_conn() -> sqlalchemy.engine.Engine:
    log.info("Connecting to PostgreSQL server")
    postgres_conn = None
    try:
        sql_alchemy_engine = create_engine(
            f"postgresql+psycopg2://{settings.POSTGRES_USER}:{settings.POSTGRES_PASS}@{settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB_NAME}"
        )

        postgres_conn = sql_alchemy_engine.connect()

        return postgres_conn
    except Exception as e:
        log.error(f"Error establishing connection to PostgreSQL server {e}")
        raise


def main() -> int:
    """Top level logic"""
    conn = database.connection(
        db_host=settings.DB_HOST,
        db_name=settings.DB_NAME,
        db_user=settings.DB_USER,
        db_password=settings.DB_PASSWORD,
        db_port=settings.DB_PORT,
    )

    connData = database.connection(
        db_host=settings.DB_HOST,
        db_name=settings.DB_NAME_PERF,
        db_user=settings.DB_USER,
        db_password=settings.DB_PASSWORD,
        db_port=settings.DB_PORT,
    )

    postgres_conn = get_postgres_conn()

    baseFundsDict = {}
    add_root_level_props(conn, baseFundsDict)
    add_profile_views(conn, baseFundsDict)
    add_inf_section(conn, baseFundsDict)
    add_re_section(conn, baseFundsDict)
    add_nr_section(conn, baseFundsDict)
    add_pd_section(conn, baseFundsDict)
    add_basic(conn, baseFundsDict, postgres_conn)
    add_hf(conn, baseFundsDict)
    add_monthly_returns(conn, settings.ES_URL, baseFundsDict)
    add_acd(conn, baseFundsDict)
    add_aum(conn, baseFundsDict)
    add_drypowder_historic(conn, baseFundsDict)
    add_drypowder(connData, baseFundsDict)
    add_target_returns(conn, baseFundsDict)
    add_structure(conn, baseFundsDict)
    add_privatecapital(conn, baseFundsDict)
    add_fund_terms(conn, baseFundsDict)
    add_listed(conn, baseFundsDict)
    add_alternativenames(conn, baseFundsDict)
    add_fundoffundsinvestment_section(conn, baseFundsDict)
    add_fundseries(conn, baseFundsDict)
    add_fund_family_details(conn, baseFundsDict)
    add_geographicfocus(conn, baseFundsDict)
    add_industryfocus(conn, baseFundsDict)
    add_pcinvestors(connData, baseFundsDict)
    add_hfinvestors(conn, baseFundsDict)
    add_service_providers(conn, baseFundsDict)
    add_fundraising_section(conn, baseFundsDict)
    add_pe_section(conn, baseFundsDict)
    add_geographic_regions(conn, baseFundsDict)
    add_open_ended_fund_perf(conn, baseFundsDict)

    log.info("Writing to json file")
    input_path = "funds.json"
    mapping_path = Path("src") / "mapping.json"

    with mapping_path.open("r") as f:
        mapping = json.load(f)

    with open(input_path, "w") as file:
        file.write('{"aliasName": "funds", "indexMapping":')
        json.dump(mapping, file)
        file.write(",\n")
        file.write('"rows": [')

        keywords = Keywords()

        first_row = True
        for row in baseFundsDict.values():
            if first_row:
                first_row = False
            else:
                file.write(",\n")

            keywords_text = []

            if "name" in row:
                keywords_text.append(row["name"])
            if "about" in row:
                keywords_text.append(row["about"])
            if "alternativeNames" in row:
                keywords_text.extend(
                    list(
                        map(
                            lambda alternative_name: alternative_name["text"]
                            if "text" in alternative_name
                            else "",
                            row["alternativeNames"],
                        )
                    )
                )
            if "basic" in row and "fundManager" in row["basic"]:
                if "name" in row["basic"]["fundManager"]:
                    keywords_text.append(row["basic"]["fundManager"]["name"])
                if "about" in row["basic"]["fundManager"]:
                    keywords_text.append(row["basic"]["fundManager"]["about"])
                if "alternativeNames" in row["basic"]["fundManager"]:
                    keywords_text.extend(
                        list(
                            map(
                                lambda alternative_name: alternative_name["text"]
                                if "text" in alternative_name
                                else "",
                                row["basic"]["fundManager"]["alternativeNames"],
                            )
                        )
                    )

            row["keywords"] = keywords.get(keywords_text, include_adjacent_words=True)

            if "industryFocus" not in row:
                row["industryFocus"] = {
                    "coreIndustries": [],
                    "industries": [],
                    "verticals": [],
                }

            has_returns = False
            if (
                "hf" in row
                and "monthlyReturns" in row["hf"]
                and row["hf"]["monthlyReturns"] is not None
            ):
                row["hf"]["monthlyReturns"] = json.loads(row["hf"]["monthlyReturns"])
                has_returns = True

            json.dump(row, file, cls=src.utils.numpy_encoder)

            if has_returns:
                # Release this memory now
                row["hf"]["monthlyReturns"] = None

            row["keywords"] = None

        file.write("]\n}\n")

    log.info("Write data to Elastisearch")
    es_writer = Writer(
        input_path,
        es_url=settings.ES_URL,
        es_swap_alias=settings.SWAP_ALIAS,
        es_request_timeout=45,
        es_chunk_size=300,
    )
    es_writer.write()

    return 0


if __name__ == "__main__":
    try:
        log.info("Starting ES Funds ETL")
        main()
        log.info("Completed ES Funds ETL")
    except Exception as e:
        log.error(f"ES Funds failed due to exception: {e}")
        sys.exit(f"ES Funds failed due to exception: {e}")
