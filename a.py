from klunk import sandbox, filters, commands, language, runtime, dataset

s = sandbox.Query('+')

datasets = s.get_datasets()

u = datasets['all']
