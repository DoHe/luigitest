import json

import luigi
import requests


class ExchangeRates(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('data/rates_%Y_%m_%d.json'))

    def run(self):
        rates = requests.get(self.date.strftime("http://api.fixer.io/%Y-%m-%d")).json()["rates"]
        with self.output().open('w') as out_file:
            out_file.write(json.dumps(rates))


class Summer(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('data/sum_%Y_%m_%d.txt'))

    def requires(self):
        return ExchangeRates(self.date)

    def run(self):
        with self.input().open('r') as in_file:
            rates = json.loads(in_file.read())
        with self.output().open('w') as out_file:
            summed = sum(rates.values())
            out_file.write(str(summed))


class MinFinder(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(self.date.strftime('data/min_%Y_%m_%d.txt'))

    def requires(self):
        return ExchangeRates(self.date)

    def run(self):
        with self.input().open('r') as in_file:
            rates = json.loads(in_file.read())
        with self.output().open('w') as out_file:
            minned = min(rates.values())
            out_file.write(str(minned))
