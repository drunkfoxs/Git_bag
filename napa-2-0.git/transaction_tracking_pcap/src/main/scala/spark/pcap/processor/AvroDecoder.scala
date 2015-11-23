package spark.pcap.processor

import kafka.serializer.Decoder
import kafka.utils.VerifiableProperties
import org.apache.avro.Schema
import org.apache.avro.io.{ BinaryDecoder, DecoderFactory, DatumReader }
import org.apache.avro.specific.{ SpecificRecordBase, SpecificDatumReader }
import org.apache.avro.file.DataFileReader


class AvroDecoder[T <: SpecificRecordBase](schema: Schema) extends Decoder[T] {
  
  private[this] val NoBinaryDecoderReuse = null.asInstanceOf[BinaryDecoder]
  private[this] val NoRecordReuse = null.asInstanceOf[T]
  
  private[this] val reader: DatumReader[T] = new SpecificDatumReader[T](schema)

  override def fromBytes(bytes: Array[Byte]): T = {
   
    val decoder = DecoderFactory.get().binaryDecoder(bytes, NoBinaryDecoderReuse)
    reader.read(NoRecordReuse, decoder)
  }
}