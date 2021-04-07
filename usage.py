from teed import bulkcm, meas

## bulkcm
stream = bulkcm.BulkCmParser.stream_to_csv("data")
bulkcm.parse("data/bulkcm.xml", "data", stream)

## meas 
meas.parse("data/mdc*xml", "data")