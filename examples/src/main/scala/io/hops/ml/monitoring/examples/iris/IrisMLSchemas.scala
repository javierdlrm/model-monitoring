package io.hops.ml.monitoring.examples.iris

object IrisMLSchemas {

  def getSchemas: (String, String) = {

    // TODO: Receive from kafka topic schema.
    // NOTE: Schema currently not included in kafka topic

    val kfkInstanceSchema =
      """{
        |  "type" : "struct",
        |  "fields" : [ {
        |    "name" : "sepal_length",
        |    "type" : "double",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "sepal_width",
        |    "type" : "double",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "petal_length",
        |    "type" : "double",
        |    "nullable" : true,
        |    "metadata" : { }
        |  }, {
        |    "name" : "petal_width",
        |    "type" : "double",
        |    "nullable" : true,
        |    "metadata" : { }
        |  } ]
        |}""".stripMargin
    val kfkReqSchema =
      """{
        |    "fields": [
        |        {
        |            "metadata": {},
        |            "name": "signature_name",
        |            "nullable": true,
        |            "type": "string"
        |        },
        |        {
        |            "metadata": {},
        |            "name": "instances",
        |            "nullable": true,
        |            "type": {
        |                "containsNull": true,
        |                "elementType": {
        |                    "containsNull": true,
        |                    "elementType": "double",
        |                    "type": "array"
        |                },
        |                "type": "array"
        |            }
        |        }
        |    ],
        |    "type": "struct"
        |}""".stripMargin
    (kfkInstanceSchema, kfkReqSchema)
  }
}
