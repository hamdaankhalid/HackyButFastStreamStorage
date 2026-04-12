
namespace StreamDB.LiteLsm;

/// <summary>
/// Metadata for a commit block within a segment file.
/// </summary>
internal readonly struct SegmentBlockInfo
{
  public readonly int BlockId { get; init; }
  public readonly int FileId { get; init; }
  public readonly long FileOffset { get; init; }
  public readonly int BlockSize { get; init; }
  public readonly int EntryCount { get; init; }

  public SegmentBlockInfo(int blockId, int fileId, long fileOffset, int blockSize, int entryCount)
  {
    BlockId = blockId;
    FileId = fileId;
    FileOffset = fileOffset;
    BlockSize = blockSize;
    EntryCount = entryCount;
  }
}
