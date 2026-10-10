-- The current slot records the delete-marker bit a restore installs.
--
-- `VersionMeta::restore_slot` consumes the version row whose kind the bit came
-- from, so `current_slot` can no longer resolve a restored marker from the row
-- at the slot's sequence: the bit must live in the slot's own record. Every
-- slot write maintains this column -- `advance_slot` derives it from the row at
-- the slot's sequence inside its own statement, `restore_slot` writes the
-- consumed row's kind -- and `current_slot` reads the row's kind when the row
-- exists, falling back to this column when it does not.
ALTER TABLE current_slots
    ADD COLUMN IF NOT EXISTS is_delete_marker boolean NOT NULL DEFAULT false;
