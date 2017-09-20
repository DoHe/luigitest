import json

import luigi
from luigi.contrib.s3 import S3Target


class S3ExampleSource(luigi.ExternalTask):
    s3source = luigi.Parameter()

    def output(self):
        return S3Target('s3://{}'.format(self.s3source))


class Keyser(luigi.Task):
    s3sink = luigi.Parameter()

    def output(self):
        return S3Target('s3://{}'.format(self.s3sink))

    def requires(self):
        return S3ExampleSource()

    def run(self):
        with self.input().open('r') as in_file:
            keys = json.loads(in_file.read()).keys()
        with self.output().open('w') as out_file:
            out_file.write('\n'.join(keys))
