/* Generated SBE (Simple Binary Encoding) message codec */
package onyx_codec;

import uk.co.real_logic.agrona.MutableDirectBuffer;

@javax.annotation.Generated(value = {"onyx_codec.MessageContainerEncoder"})
@SuppressWarnings("all")
public class MessageContainerEncoder
{
    public static final int BLOCK_LENGTH = 22;
    public static final int TEMPLATE_ID = 0;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final MessageContainerEncoder parentMessage = this;
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

    public MessageContainerEncoder wrap(final MutableDirectBuffer buffer, final int offset)
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

    public MessageContainerEncoder replicaVersion(final long value)
    {
        buffer.putInt(offset + 0, (int)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int fromPeerNumNullValue()
    {
        return 65535;
    }

    public static int fromPeerNumMinValue()
    {
        return 0;
    }

    public static int fromPeerNumMaxValue()
    {
        return 65534;
    }

    public MessageContainerEncoder fromPeerNum(final int value)
    {
        buffer.putShort(offset + 4, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
        return this;
    }


    public static int toTaskNumsNullValue()
    {
        return 65535;
    }

    public static int toTaskNumsMinValue()
    {
        return 0;
    }

    public static int toTaskNumsMaxValue()
    {
        return 65534;
    }


    public static int toTaskNumsLength()
    {
        return 8;
    }

    public void toTaskNums(final int index, final int value)
    {
        if (index < 0 || index >= 8)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        final int pos = this.offset + 6 + (index * 2);
        buffer.putShort(pos, (short)value, java.nio.ByteOrder.LITTLE_ENDIAN);
    }

    public static int payloadId()
    {
        return 24;
    }

    public static String payloadCharacterEncoding()
    {
        return "UTF-8";
    }

    public static String payloadMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
    }

    public static int payloadHeaderLength()
    {
        return 4;
    }

    public MessageContainerEncoder putPayload(
        final uk.co.real_logic.agrona.DirectBuffer src, final int srcOffset, final int length)
    {
        if (length > 2147483647)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, (int)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public MessageContainerEncoder putPayload(
        final byte[] src, final int srcOffset, final int length)
    {
        if (length > 2147483647)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, (int)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, src, srcOffset, length);

        return this;
    }

    public MessageContainerEncoder payload(final String value)
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
        if (length > 2147483647)
        {
            throw new IllegalArgumentException("length > max value for type: " + length);
        }

        final int headerLength = 4;
        final int limit = parentMessage.limit();
        parentMessage.limit(limit + headerLength + length);
        buffer.putInt(limit, (int)length, java.nio.ByteOrder.LITTLE_ENDIAN);
        buffer.putBytes(limit + headerLength, bytes, 0, length);

        return this;
    }
}
