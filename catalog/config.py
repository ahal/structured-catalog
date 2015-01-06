from ConfigParser import ConfigParser
import os


globals()['settings'] = {
    'datastore': 'elasticsearch',
    'structured_log_names': ['raw_structured_logs.log', 'wpt_structured_full.log'],
    'work_queues': ['sqs', 'mongo'],
}
globals()['pulse'] = {}
globals()['database'] = {}

def read_runtime_config(config_path):
    if not os.path.isfile(config_path):
        raise OSError("Invalid file path '{}'!".format(config_path))

    cp = ConfigParser()
    cp.read(config_path)
    for section in cp.sections():
        if section == 'settings':
            items = dict([(k.upper(), v) for k, v in cp.items(section)])
        else:
            items = dict(cp.items(section))

        if section in globals():
            globals()[section].update(items)
        else:
            globals()[section] = items

        for k, v in globals()[section].iteritems():
            try:
                globals()[section][k] = int(v)
            except:
                pass
