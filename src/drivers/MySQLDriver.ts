import mysql, { Pool, PoolOptions, PoolConnection } from 'mysql2/promise';
import { IDriver, Query, Data } from '../interfaces/IDriver';

/**
 * MySQL driver configuration options
 */
export interface MySQLDriverOptions {
    /** MySQL host */
    host: string;
    /** MySQL port (default: 3306) */
    port?: number;
    /** MySQL user */
    user: string;
    /** MySQL password */
    password: string;
    /** MySQL database name */
    database: string;
    /** Table name to use */
    tableName: string;
    /** Connection pool size (default: 10) */
    connectionLimit?: number;
    /** Additional MySQL connection options */
    connectionOptions?: PoolOptions;
}

/**
 * Driver implementation for MySQL using mysql2.
 * Supports Hybrid Schema (Real Columns + JSON) for optimized storage and querying.
 */
export class MySQLDriver implements IDriver {
    private pool: Pool | null = null;
    private host: string;
    private port: number;
    private user: string;
    private password: string;
    private database: string;
    private tableName: string;
    private connectionLimit: number;
    private connectionOptions: PoolOptions;
    private schema: any = null;

    /**
     * Database type (sql or nosql).
     * Defaults to 'nosql' until a schema is set via setSchema().
     */
    public dbType: 'sql' | 'nosql' = 'nosql';

    /**
     * Creates a new instance of MySQLDriver
     * @param options - MySQL driver configuration options
     */
    constructor(options: MySQLDriverOptions) {
        this.host = options.host;
        this.port = options.port || 3306;
        this.user = options.user;
        this.password = options.password;
        this.database = options.database;
        this.tableName = options.tableName;
        this.connectionLimit = options.connectionLimit || 10;
        this.connectionOptions = options.connectionOptions || {};
    }

    /**
     * Sets the schema for the driver.
     * Switches the driver to SQL mode to use real columns.
     * @param schema - The schema instance
     */
    setSchema(schema: any): void {
        this.schema = schema;
        this.dbType = 'sql';
    }

    /**
     * Connects to the MySQL database.
     * Creates the table (Hybrid or JSON only) if it doesn't exist.
     */
    async connect(): Promise<void> {
        this.pool = mysql.createPool({
            host: this.host,
            port: this.port,
            user: this.user,
            password: this.password,
            database: this.database,
            connectionLimit: this.connectionLimit,
            waitForConnections: true,
            ...this.connectionOptions,
        });

        let createTableSQL = '';

        if (this.schema && this.dbType === 'sql') {
            const definition = typeof this.schema.getDefinition === 'function' 
                ? this.schema.getDefinition() 
                : this.schema;

            // Map Schema Types to SQL Types
            const columns = Object.entries(definition).map(([key, type]) => {
                const sqlType = this.mapHawiahTypeToSQL(type);
                return `\`${key}\` ${sqlType}`; // Using backticks for safe column names
            }).join(',\n                ');

            // Hybrid Table: Real Columns + _extras JSON
            createTableSQL = `
                CREATE TABLE IF NOT EXISTS \`${this.tableName}\` (
                    _id VARCHAR(100) PRIMARY KEY,
                    ${columns},
                    _extras JSON,
                    _createdAt DATETIME NOT NULL,
                    _updatedAt DATETIME NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `;
        } else {
            // NoSQL Mode: JSON Store
            createTableSQL = `
                CREATE TABLE IF NOT EXISTS \`${this.tableName}\` (
                    _id VARCHAR(100) PRIMARY KEY,
                    _data JSON NOT NULL,
                    _createdAt DATETIME NOT NULL,
                    _updatedAt DATETIME NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            `;
        }

        const connection = await this.pool.getConnection();
        try {
            await connection.execute(createTableSQL);
            // Index Creation (Standard columns only)
            // MySQL 5.7+ supports index on generated columns for JSON, but here we cover basics
            try {
                await connection.execute(`CREATE INDEX idx_createdAt ON \`${this.tableName}\` (_createdAt)`);
                await connection.execute(`CREATE INDEX idx_updatedAt ON \`${this.tableName}\` (_updatedAt)`);
            } catch (e) {
                // Ignore if index already exists
            }
        } finally {
            connection.release();
        }
    }

    /**
     * Disconnects from the MySQL database.
     */
    async disconnect(): Promise<void> {
        if (this.pool) {
            await this.pool.end();
            this.pool = null;
        }
    }

    /**
     * Inserts a new record into the database.
     * Uses real columns if schema is present.
     * @param data - The data to insert
     * @returns The inserted record with ID
     */
    async set(data: Data): Promise<Data> {
        this.ensureConnected();

        const id = this.generateId();
        const now = new Date();
        const record = {
            ...data,
            _id: id,
            _createdAt: now.toISOString(),
            _updatedAt: now.toISOString(),
        };

        if (this.schema && this.dbType === 'sql') {
            const { schemaData, extraData } = this.splitData(record);
            const schemaKeys = Object.keys(schemaData);
            const schemaValues = Object.values(schemaData);

            // Columns: _id, [schema columns], _extras, _createdAt, _updatedAt
            const cols = ['_id', ...schemaKeys, '_extras', '_createdAt', '_updatedAt'];
            const placeholders = cols.map(() => '?').join(', ');
            
            // Values need formatting for MySQL
            const values = [id, ...schemaValues, JSON.stringify(extraData), now, now];
            // Format column names with backticks
            const colString = cols.map(c => `\`${c}\``).join(', ');

            const sql = `INSERT INTO \`${this.tableName}\` (${colString}) VALUES (${placeholders})`;
            
            await this.pool!.execute(sql, values);

        } else {
            const sql = `
                INSERT INTO \`${this.tableName}\` (_id, _data, _createdAt, _updatedAt)
                VALUES (?, ?, ?, ?)
            `;
            await this.pool!.execute(sql, [id, JSON.stringify(record), now, now]);
        }

        return record;
    }

    /**
     * Retrieves records matching the query.
     * @param query - The query criteria
     * @returns Array of matching records
     */
    async get(query: Query): Promise<Data[]> {
        this.ensureConnected();

        if (this.schema && this.dbType === 'sql') {
            const sql = `SELECT * FROM \`${this.tableName}\``;
            const [rows] = await this.pool!.execute(sql);

            // Merge Real Columns + Extras JSON
            const records = (rows as any[]).map(row => this.mergeData(row));

            if (Object.keys(query).length === 0) return records;
            return records.filter(record => this.matchesQuery(record, query));

        } else {
            const sql = `SELECT _data FROM \`${this.tableName}\``;
            const [rows] = await this.pool!.execute(sql);

            const allRecords = (rows as any[]).map(row =>
                typeof row._data === 'string' ? JSON.parse(row._data) : row._data
            );

            if (Object.keys(query).length === 0) return allRecords;
            return allRecords.filter(record => this.matchesQuery(record, query));
        }
    }

    /**
     * Retrieves a single record matching the query.
     * @param query - The query criteria
     * @returns The first matching record or null
     */
    async getOne(query: Query): Promise<Data | null> {
        this.ensureConnected();

        if (query._id) {
            if (this.schema && this.dbType === 'sql') {
                const sql = `SELECT * FROM \`${this.tableName}\` WHERE _id = ? LIMIT 1`;
                const [rows] = await this.pool!.execute(sql, [query._id]);
                return (rows as any[]).length > 0 ? this.mergeData((rows as any[])[0]) : null;
            } else {
                const sql = `SELECT _data FROM \`${this.tableName}\` WHERE _id = ? LIMIT 1`;
                const [rows] = await this.pool!.execute(sql, [query._id]);
                if ((rows as any[]).length > 0) {
                    const row = (rows as any[])[0];
                    return typeof row._data === 'string' ? JSON.parse(row._data) : row._data;
                }
                return null;
            }
        }

        const results = await this.get(query);
        return results.length > 0 ? results[0] : null;
    }

    /**
     * Updates records matching the query.
     * @param query - The query criteria
     * @param data - The data to update
     * @returns The number of updated records
     */
    async update(query: Query, data: Data): Promise<number> {
        this.ensureConnected();

        const records = await this.get(query);
        let count = 0;

        for (const record of records) {
            const updatedRecord: any = {
                ...record,
                ...data,
                _updatedAt: new Date().toISOString(),
            };

            const now = new Date();

            if (this.schema && this.dbType === 'sql') {
                const { schemaData, extraData } = this.splitData(updatedRecord);
                const schemaKeys = Object.keys(schemaData);
                const schemaValues = Object.values(schemaData);

                // Build SET clause: `col1` = ?, `col2` = ?
                let setParts = [];
                let params = [];
                
                for (let i = 0; i < schemaKeys.length; i++) {
                    setParts.push(`\`${schemaKeys[i]}\` = ?`);
                    params.push(schemaValues[i]);
                }

                setParts.push(`\`_extras\` = ?`);
                params.push(JSON.stringify(extraData));

                setParts.push(`\`_updatedAt\` = ?`);
                params.push(now);

                params.push(record._id); // WHERE clause param

                const sql = `UPDATE \`${this.tableName}\` SET ${setParts.join(', ')} WHERE _id = ?`;
                await this.pool!.execute(sql, params);

            } else {
                const sql = `
                    UPDATE \`${this.tableName}\`
                    SET _data = ?, _updatedAt = ?
                    WHERE _id = ?
                `;
                await this.pool!.execute(sql, [
                    JSON.stringify(updatedRecord),
                    now,
                    record._id,
                ]);
            }
            count++;
        }

        return count;
    }

    /**
     * Deletes records matching the query.
     * @param query - The query criteria
     * @returns The number of deleted records
     */
    async delete(query: Query): Promise<number> {
        this.ensureConnected();
        const records = await this.get(query);
        const sql = `DELETE FROM \`${this.tableName}\` WHERE _id = ?`;

        let count = 0;
        for (const record of records) {
            await this.pool!.execute(sql, [record._id]);
            count++;
        }
        return count;
    }

    /**
     * Checks if any record matches the query.
     * @param query - The query criteria
     * @returns True if a match exists, false otherwise
     */
    async exists(query: Query): Promise<boolean> {
        this.ensureConnected();
        const result = await this.getOne(query);
        return result !== null;
    }

    /**
     * Counts records matching the query.
     * @param query - The query criteria
     * @returns The number of matching records
     */
    async count(query: Query): Promise<number> {
        this.ensureConnected();

        if (Object.keys(query).length === 0) {
            const sql = `SELECT COUNT(*) as count FROM \`${this.tableName}\``;
            const [rows] = await this.pool!.execute(sql);
            return (rows as any[])[0].count;
        }

        const results = await this.get(query);
        return results.length;
    }

    /**
     * Ensures the database is connected before executing operations.
     * @throws Error if database is not connected
     * @private
     */
    private ensureConnected(): void {
        if (!this.pool) {
            throw new Error('Database not connected. Call connect() first.');
        }
    }

    /**
     * Generates a unique ID for records.
     * @returns A unique string ID
     * @private
     */
    private generateId(): string {
        return `${Date.now()}_${Math.random().toString(36).substring(2, 15)}`;
    }

    /**
     * Checks if a record matches the query criteria.
     * @param record - The record to check
     * @param query - The query criteria
     * @returns True if the record matches
     * @private
     */
    private matchesQuery(record: Data, query: Query): boolean {
        for (const [key, value] of Object.entries(query)) {
            if (record[key] !== value) {
                return false;
            }
        }
        return true;
    }

    /**
     * Maps Hawiah types to MySQL types.
     * @param type - The Hawiah schema type
     * @returns The corresponding MySQL type string
     * @private
     */
    private mapHawiahTypeToSQL(type: any): string {
        let t = type;
        if (typeof type === 'object' && type !== null && type.type) {
            t = type.type;
        }
        t = String(t).toUpperCase();

        if (t.includes('STRING') || t.includes('TEXT') || t.includes('EMAIL') || t.includes('URL') || t.includes('CHAR') || t.includes('UUID')) return 'TEXT';
        if (t.includes('NUMBER')) {
            if (t.includes('INT')) return 'INT';
            if (t.includes('BIGINT')) return 'BIGINT';
            return 'DOUBLE'; // or FLOAT
        }
        if (t.includes('BOOLEAN')) return 'BOOLEAN'; // MySQL converts to TINYINT(1)
        if (t.includes('DATE')) return 'DATETIME';
        if (t.includes('JSON')) return 'JSON';
        if (t.includes('BLOB')) return 'BLOB';

        return 'TEXT';
    }

    /**
     * Splits data into schema columns and extra data.
     * @param data - The full data object
     * @returns Object containing separate schemaData and extraData
     * @private
     */
    private splitData(data: Data): { schemaData: Data, extraData: Data } {
        if (!this.schema) return { schemaData: {}, extraData: data };

        const definition = typeof this.schema.getDefinition === 'function'
            ? this.schema.getDefinition()
            : this.schema;

        const schemaData: Data = {};
        const extraData: Data = {};

        for (const [key, value] of Object.entries(data)) {
            if (key in definition) {
                schemaData[key] = value;
            } else if (!['_id', '_createdAt', '_updatedAt'].includes(key)) {
                extraData[key] = value;
            }
        }
        return { schemaData, extraData };
    }

    /**
     * Merges schema columns and extra data back into a single object.
     * @param row - The raw database row
     * @returns The fully merged data object
     * @private
     */
    private mergeData(row: any): Data {
        const { _extras, ...rest } = row;
        // _extras is a JSON type in MySQL, mysql2 client handles parsing if configured correctly.
        // But often returns as object directly.
        const extras = (typeof _extras === 'object' && _extras !== null) ? _extras : 
                       (typeof _extras === 'string' ? JSON.parse(_extras) : {});
        return { ...rest, ...extras };
    }

    // --- Public Utility Methods ---

    /**
     * Gets the MySQL connection pool.
     * @returns The MySQL connection pool
     */
    getPool(): Pool | null {
        return this.pool;
    }

    /**
     * Executes a raw SQL query.
     * WARNING: Use with caution. This bypasses the abstraction layer.
     * @param sql - The SQL query to execute
     * @param params - Optional parameters for the query
     * @returns Query results
     */
    async executeRaw(sql: string, params?: any[]): Promise<any> {
        this.ensureConnected();
        const [rows] = await this.pool!.execute(sql, params);
        return rows;
    }

    /**
     * Clears all data from the table.
     */
    async clear(): Promise<void> {
        this.ensureConnected();
        await this.pool!.execute(`DELETE FROM \`${this.tableName}\``);
    }

    /**
     * Drops the entire table.
     * WARNING: This will permanently delete all data and indexes.
     */
    async drop(): Promise<void> {
        this.ensureConnected();
        await this.pool!.execute(`DROP TABLE IF EXISTS \`${this.tableName}\``);
    }

    /**
     * Optimizes the table.
     */
    async optimize(): Promise<void> {
        this.ensureConnected();
        await this.pool!.execute(`OPTIMIZE TABLE \`${this.tableName}\``);
    }

    /**
     * Analyzes the table for query optimization.
     */
    async analyze(): Promise<void> {
        this.ensureConnected();
        await this.pool!.execute(`ANALYZE TABLE \`${this.tableName}\``);
    }

    /**
     * Begins a transaction.
     * @returns A connection from the pool for transaction use
     */
    async beginTransaction(): Promise<PoolConnection> {
        this.ensureConnected();
        const connection = await this.pool!.getConnection();
        await connection.beginTransaction();
        return connection;
    }

    /**
     * Commits a transaction.
     * @param connection - The connection to commit
     */
    async commit(connection: PoolConnection): Promise<void> {
        await connection.commit();
        connection.release();
    }

    /**
     * Rolls back a transaction.
     * @param connection - The connection to rollback
     */
    async rollback(connection: PoolConnection): Promise<void> {
        await connection.rollback();
        connection.release();
    }
}