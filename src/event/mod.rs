

/// An `EventLog` provides a log of events associated with a specific object.
pub trait EventLog{
    /// Formats its arguments with fmt.Sprintf and adds the
    /// result to the event log.
    fn printf(&self);

    /// Like printf. but it marks this event as an error.
    fn errorf(&self);

    /// Declares that this event log is complete.
    /// The event log should not be used after calling this method.
    fn finish(&self);
}