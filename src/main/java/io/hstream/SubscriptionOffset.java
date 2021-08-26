package io.hstream;

public class SubscriptionOffset {
  public enum SpecialOffset {
    EARLIEST(io.hstream.internal.SubscriptionOffset.SpecialOffset.EARLIST),
    LATEST(io.hstream.internal.SubscriptionOffset.SpecialOffset.LATEST),
    UNRECOGNIZED(io.hstream.internal.SubscriptionOffset.SpecialOffset.UNRECOGNIZED);

    private final io.hstream.internal.SubscriptionOffset.SpecialOffset rep;

    SpecialOffset(io.hstream.internal.SubscriptionOffset.SpecialOffset offset) {
      this.rep = offset;
    }

    static SpecialOffset specialOffsetFromGrpc(
        io.hstream.internal.SubscriptionOffset.SpecialOffset offset) {
      switch (offset) {
        case EARLIST:
          return EARLIEST;
        case LATEST:
          return LATEST;
        case UNRECOGNIZED:
          return UNRECOGNIZED;
        default:
          throw new IllegalArgumentException();
      }
    }

    public io.hstream.internal.SubscriptionOffset.SpecialOffset getRep() {
      return this.rep;
    }
  }

  private final io.hstream.internal.SubscriptionOffset rep;

  public SubscriptionOffset(io.hstream.internal.SubscriptionOffset rep) {
    this.rep = rep;
  }

  public SubscriptionOffset(SpecialOffset offset) {
    this.rep =
        io.hstream.internal.SubscriptionOffset.newBuilder()
            .setSpecialOffset(offset.getRep())
            .build();
  }

  public static SubscriptionOffset subscriptionOffsetFromGrpc(
      io.hstream.internal.SubscriptionOffset offset) {
    return new SubscriptionOffset(offset);
  }

  public io.hstream.internal.SubscriptionOffset getRep() {
    return this.rep;
  }

  public SpecialOffset getOffset() {
    return SpecialOffset.specialOffsetFromGrpc(this.rep.getSpecialOffset());
  }
}
