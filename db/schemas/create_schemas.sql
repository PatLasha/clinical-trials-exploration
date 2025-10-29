--create schemas
CREATE SCHEMA IF NOT EXISTS staging; -- staging schema for raw data
CREATE SCHEMA IF NOT EXISTS processed; -- processed schema for cleaned data
CREATE SCHEMA IF NOT EXISTS analytics; -- analytics schema for processed data

-- Grant usage on schemas
DO $$
BEGIN
    EXECUTE format('GRANT ALL ON SCHEMA staging TO %I', current_user);
    EXECUTE format('GRANT ALL ON SCHEMA processed TO %I', current_user);
    EXECUTE format('GRANT ALL ON SCHEMA analytics TO %I', current_user);
END $$;