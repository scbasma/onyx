/* Generated SBE (Simple Binary Encoding) message codec */
package onyx_codec;

import uk.co.real_logic.agrona.MutableDirectBuffer;

@javax.annotation.Generated(value = {"onyx_codec.BarrierEncoder"})
@SuppressWarnings("all")
public class BarrierEncoder
{
    public static final int BLOCK_LENGTH = 8;
    public static final int TEMPLATE_ID = 25;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final BarrierEncoder parentMessage = this;
    private MutableDirectBuffer buffer;
    protected int offset;
    protected int limit;
    protected int actingBlockLength;
    protected int actingVersion;

    public int sbeBlockLength()
    {
        return BLOCK_LENGTH;
    }

    public int sbeTemplateId()
    {
        return TEMPLATE_ID;
    }

    public int sbeSchemaId()
    {
        return SCHEMA_ID;
    }

    public int sbeSchemaVersion()
    {
        return SCHEMA_VERSION;
    }

    public String sbeSemanticType()
    {
        return "";
    }

    public int offset()
    {
        return offset;
    }

    public BarrierEncoder wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;
        limit(offset + BLOCK_LENGTH);
        return this;
    }

    public int encodedLength()
    {
        return limit - offset;
    }

    public int limit()
    {
        return limit;
    }

    public void limit(final int limit)
    {
        this.limit = limit;
    }

    public static int fromTaskNumNullValue()
    {
        return 65535;
    }

    public static int fromTaskNumMinValue()
    {
        return 0;
    }

    public static int fromTaskNumMaxValue()
    {
        return 65534;
    }

    public BarrierEncoder fromTaskNum(final int value)
    {
        buffer.putShort(offset + 0, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int toTaskNumNullValue()
    {
        return 65535;
    }

    public static int toTaskNumMinValue()
    {
        return 0;
    }

    public static int toTaskNumMaxValue()
    {
        return 65534;
    }

    public BarrierEncoder toTaskNum(final int value)
    {
        buffer.putShort(offset + 2, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static long replicaVersionNullValue()
    {
        return 4294967294L;
    }

    public static long replicaVersionMinValue()
    {
        return 0L;
    }

    public static long replicaVersionMaxValue()
    {
        return 4294967293L;
    }

    public BarrierEncoder replicaVersion(final long value)
    {
        buffer.putInt(offset + 4, (int)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int replicaId()
    {
        return 28;
    }

    public static String replicaCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String replicaMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int replicaHeaderLength()
    {
        return 2;
    }

    public BarrierEncoder putReplica(
        final uk.co.real_logic.agrona.DirectBuffer src, final int srcOffset, final int length)
    {
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public BarrierEncoder putReplica(
        final byte[] src, final int srcOffset, final int length)
    {
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public BarrierEncoder replica(final String value)
    {
        final byte[] bytes;
        try
        {
            bytes = value.getBytes("UTF-8");
        }
        catch (final java.io.UnsupportedEncodingException ex)
        {
            throw new RuntimeException(ex);
        }

        final int length = bytes.length;
        if (length > 65534)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 2;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putShort(limit, (short)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, bytes, 0, length);

        return this;
    }
}
