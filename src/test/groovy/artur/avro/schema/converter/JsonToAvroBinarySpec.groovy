package artur.avro.schema.converter

import spock.lang.Specification

class JsonToAvroBinarySpec extends Specification {

    def jsonToAvroBinaryConverter = new JsonToAvroBinary()

    def "should convert to binary avro with record" () {
        given:
        def avroSchema = '''
            {
                "type" : "record",
                "name" : "avroTestSchema",
                "fields" : [
                    {
                      "name" : "testFieldString",
                      "type" : "string",
                      "default": null
                    },
                    {
                      "name" : "testFieldInteger",
                      "type" : "int",
                      "default": null
                    }
                ]
            }
        '''

        def jsonData = '''
            {
                "testFieldString" : "test String abcdef",
                "testFieldInteger" : 123
            }
        '''
        when:
        byte[] avroBinary = jsonToAvroBinaryConverter.convert(avroSchema, jsonData)

        then:
        assert avroBinary != null
    }
}

