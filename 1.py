import pandas

inputs = pandas.read_csv('inputs.csv')
outputs = pandas.read_csv('outputs.csv')
tags = pandas.read_csv('tags.csv')
transactions = pandas.read_csv('transactions')

inputs.columns = ["id", "tx_id", "sig_id", "output_id"]
outputs.columns = ["id", "tx_id", "pk_id", "value"]
tags.columns = ["type", "service", "pk_id"]
transactions = ["id", "block_id"]

