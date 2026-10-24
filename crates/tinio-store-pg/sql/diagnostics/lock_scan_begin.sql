-- The doctor's Object Lock scan (OL31) reads its three tables in one
-- repeatable-read snapshot, so a concurrent write cannot make a lock row look
-- orphaned between two statements of the same scan.
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY
