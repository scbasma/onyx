/* Generated SBE (Simple Binary Encoding) message codec */
package onyx_codec;

import uk.co.real_logic.agrona.DirectBuffer;

@javax.annotation.Generated(value = {"onyx_codec.MessageContainerDecoder"})
@SuppressWarnings("all")
public class MessageContainerDecoder
{
    public static final int BLOCK_LENGTH = 22;
    public static final int TEMPLATE_ID = 0;
    public static final int SCHEMA_ID = 1;
    public static final int SCHEMA_VERSION = 0;

    private final MessageContainerDecoder parentMessage = this;
    private DirectBuffer buffer;
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

    public MessageContainerDecoder wrap(
        final DirectBuffer buffer, final int offset, final int actingBlockLength, final int actingVersion)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.actingBlockLength = actingBlockLength;
        this.actingVersion = actingVersion;
        limit(offset + actingBlockLength);

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

    public static int replicaVersionId()
    {
        return 1;
    }

    public static String replicaVersionMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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


    public long replicaVersion()
    {
        return (buffer.getInt(offset + 0, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFF);
    }


    public static int fromPeerNumId()
    {
        return 22;
    }

    public static String fromPeerNumMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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


    public int fromPeerNum()
    {
        return (buffer.getShort(offset + 4, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF);
    }


    public static int toTaskNumsId()
    {
        return 23;
    }

    public static String toTaskNumsMetaAttribute(final MetaAttribute metaAttribute)
    {
        switch (metaAttribute)
        {
            case EPOCH: return "unix";
            case TIME_UNIT: return "nanosecond";
            case SEMANTIC_TYPE: return "";
        }

        return "";
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

    public int toTaskNums(final int index)
    {
        if (index < 0 || index >= 8)
        {
            throw new IndexOutOfBoundsException("index out of range: index=" + index);
        }

        final int pos = this.offset + 6 + (index * 2);
        return (buffer.getShort(pos, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF);
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

    public int payloadLength()
    {
        final int limit = parentMessage.limit();
        return (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFF);
    }

    public int getPayload(
        final uk.co.real_logic.agrona.MutableDirectBuffer dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFF);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public int getPayload(
        final byte[] dst, final int dstOffset, final int length)
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFF);
        final int bytesCopied = Math.min(length, dataLength);
        parentMessage.limit(limit + headerLength + dataLength);
        buffer.getBytes(limit + headerLength, dst, dstOffset, bytesCopied);

        return bytesCopied;
    }

    public String payload()
    {
        final int headerLength = 4;
        final int limit = parentMessage.limit();
        final int dataLength = (buffer.getInt(limit, java.nio.ByteOrder.LITTLE_ENDIAN) & 0xFFFF_FFFF);
        parentMessage.limit(limit + headerLength + dataLength);
        final byte[] tmp = new byte[dataLength];
        buffer.getBytes(limit + headerLength, tmp, 0, dataLength);

        final String value;
        try
        {
            value = new String(tmp, "UTF-8");
        }
        catch (final java.io.UnsupportedEncodingException ex)
        {
            throw new RuntimeException(ex);
        }

        return value;
    }
}
