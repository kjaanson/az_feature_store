import json

from pathlib import Path

conf_path = Path(__file__).parents[0].with_name('local.settings.json')

config = json.loads(conf_path.open('r').read())


