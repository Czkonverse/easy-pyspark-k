import click

from common.sparksession import init_sparksession
from utils import get_data_from_hive


@click.command()
@click.option('-input_date', '--input_date', help='input date')
def run(input_date):
    # 0 initialize SparkSession
    spark = init_sparksession()

    # 1 get data from hive
    df = get_data_from_hive(spark, input_date)


if __name__ == '__main__':
    run()
