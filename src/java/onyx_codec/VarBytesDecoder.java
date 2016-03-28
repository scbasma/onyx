/* Generated SBE (Simple Binary Encoding) message codec */
package onyx_codec;

import uk.co.real_logic.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx_codec.VarBytesDecoder"})
@SuppressWarnings("all")
public class VarBytesDecoder
{
    public static final int ENCODED_LENGTH = -1;
    private DirectBuffer buffer;
    private int offset;

    public VarBytesDecoder wrap(final DirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        return this;
    }

    public int encodedLength()
    {
        return ENCODED_LENGTH;
    }

    public static long lengthNullValue()
    {
        return 4294967294L;
    }

    public static long lengthMinValue()
    {
        return 0L;
    }

    public static long lengthMaxValue()
    {
        return 2147483647L;
    }


    public long length()
    {
        return (buffer.getInt(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFF);
    }


    public static byte varDataNullValue()
    {
        return (byte)-128;
    }

    public static byte varDataMinValue()
    {
        return (byte)-127;
    }

    public static byte varDataMaxValue()
    {
        return (byte)127;
    }

}
