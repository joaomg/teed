from pprint import pprint
from frictionless import Package, Resource


def bulkcm_to_csv(source: Package):
    print(f"Create data-package from {source.name}")
    pprint(source.resources)

    # target = Package(resources=[Resource(name="bulkcm", path="data/bulkcm.xml")])
    # return target


source = Package(
    name="bulkcm-package", resources=[Resource(name="bulkcm", path="data/bulkcm.xml")]
)

target = bulkcm_to_csv(source)
pprint(target)

# pprint(target.resource_names)
# pprint(target.get_resource('bulkcm').schema)
# pprint(target.get_resource("new").read_rows())
