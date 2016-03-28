/* Generated SBE (Simple Binary Encoding) message codec */
package onyx_codec;

import uk.co.real_logic.agrona.MutableDirectBuffer;

@javax.annotation.Generated(value = {"onyx_codec.VarBytesEncoder"})
@SuppressWarnings("all")
public class VarBytesEncoder
{
    public static final int ENCODED_LENGTH = -1;
    private MutableDirectBuffer buffer;
    private int offset;

    public VarBytesEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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

    public VarBytesEncoder length(final long value)
    {
        buffer.putInt(offset + 0, (int)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
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
