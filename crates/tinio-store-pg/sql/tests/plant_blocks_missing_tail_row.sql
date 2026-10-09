-- Drop one chunk row of a stored block, behind the store's back: the block's
-- declared length row survives, so a reader sees fewer bytes than it promises.
DELETE FROM blocks
WHERE hash = $1 AND seq = $2
