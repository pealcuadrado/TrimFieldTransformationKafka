# TrimFieldTransformationKafka
Java class to perform Kafka Connect transformation. This class implements a Trim function over fields on Key or value

## USAGE
1. Generate JAR package, put it in Kafka Connect classpath and restart Kafka Connect service.
2. Configure your connector json file properties adding the following lines:

**To edit the message Value:**

> "transforms": "TrimCustom",
> "transforms.TrimCustom.type": "org.pealcuadrado.kafka.connect.transforms.TrimField$Value",
> "transforms.TrimCustom.fields": "field_to_trim1,field_to_trim2"

**To edit the message Key:**

> "transforms": "TrimCustom",
> "transforms.TrimCustom.type": "org.pealcuadrado.kafka.connect.transforms.TrimField$Key",
> "transforms.TrimCustom.fields": "field_to_trim1,field_to_trim2",
