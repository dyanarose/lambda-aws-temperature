import unittest
import json
import temperature_from_dynamo as dyn


class TestTemperatureHandler(unittest.TestCase):
    def test_filter_for_inserts(self):
        result = dyn.get_inserts(self.event['Records'])
        self.assertEqual(1, len(result))

    def test_convert_for_sqs(self):
        reference = {'time': '1', 'device': 'aws-console', 'temperature': '1.456'}
        insert = dyn.get_inserts(self.event['Records'])[0]

        sqs_data = dyn.convert_to_sqs(insert)
        self.assertDictEqual(reference, sqs_data)

    def setUp(self):
        objs = '''{
        "Records":[{
                    "eventID":"1",
                    "eventName":"INSERT",
                    "eventVersion":"1.0",
                    "eventSource":"aws:dynamodb",
                    "awsRegion":"us-east-1",
                    "dynamodb":{
                                "Keys":{
                                        "Id":{
                                              "N":"101"
                                             }
                                       },

                               "NewImage":{
                                            "device": {
                                              "S": "aws-console"
                                            },
                                            "temperature": {
                                              "S": "1.456"
                                            },
                                            "time": {
                                              "N": "1"
                                            }
                                },
                                "SequenceNumber":"111",
                                "SizeBytes":26,
                                "StreamViewType":"NEW_AND_OLD_IMAGES"
                               },
                                 "eventSourceARN":"stream-ARN"
                     },
                     {
                      "eventID":"2",
                      "eventName":"MODIFY",
                      "eventVersion":"1.0",
                      "eventSource":"aws:dynamodb",
                      "awsRegion":"us-east-1",
                      "dynamodb":{
                                  "Keys":{
                                          "Id":{
                                                "N":"101"
                                               }
                                         },
                                   "NewImage":{
                                               "Message":{
                                                          "S":"This item has changed"
                                                         },
                                               "Id":{
                                                     "N":"101"
                                                    }
                                              },
                                   "OldImage":{
                                               "Message":{
                                                          "S":"New item!"
                                                         },
                                               "Id":{
                                                     "N":"101"
                                                    }
                                               },
                                   "SequenceNumber":"222",
                                   "SizeBytes":59,
                                   "StreamViewType":"NEW_AND_OLD_IMAGES"
                                  },
                        "eventSourceARN":"stream-ARN"
                 },
                 {
                  "eventID":"3",
                  "eventName":"REMOVE",
                  "eventVersion":"1.0",
                  "eventSource":"aws:dynamodb",
                  "awsRegion":"us-east-1",
                  "dynamodb":{
                              "Keys":{
                                      "Id":{
                                            "N":"101"
                                           }
                                     },
                                      "OldImage":{
                                                  "Message":{
                                                             "S":"This item has changed"
                                                            },
                                                  "Id":{
                                                        "N":"101"
                                                       }
                                                  },
                                       "SequenceNumber":"333",
                                       "SizeBytes":38,
                                       "StreamViewType":"NEW_AND_OLD_IMAGES"
                             },
                  "eventSourceARN":"stream-ARN"
                 }
                ]
              }'''

        self.event = json.loads(objs)

if __name__ == '__main__':
    unittest.main()
