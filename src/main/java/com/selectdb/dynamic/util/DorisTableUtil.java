// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.selectdb.dynamic.util;


import com.selectdb.dynamic.SourceSchema;
import com.selectdb.dynamic.TableSchema;
import com.selectdb.dynamic.doris.DorisSchemaFactory;
import com.selectdb.dynamic.doris.DorisSystem;
import com.selectdb.dynamic.doris.DorisTableConfig;
import com.selectdb.dynamic.exception.CreateTableException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.ArrayList;

/** Utility class for Doris table operations. */
public class DorisTableUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DorisTableUtil.class);


    public static void tryCreateTableIfAbsent(
            DorisSystem dorisSystem,
            String targetDb,
            String dorisTable,
            SourceSchema schema,
            DorisTableConfig tableConfig,
            boolean ignoreIncompatible)
            throws CreateTableException {

        if (!dorisSystem.tableExists(targetDb, dorisTable)) {
            if (tableConfig.isConvertUniqToPk()
                    && CollectionUtil.isNullOrEmpty(schema.primaryKeys)
                    && CollectionUtil.isNullOrEmpty(schema.uniqueIndexs)) {
                schema.primaryKeys = new ArrayList<>(schema.uniqueIndexs);
            }

            TableSchema dorisSchema =
                    DorisSchemaFactory.createTableSchema(
                            targetDb,
                            dorisTable,
                            schema.getFields(),
                            schema.getPrimaryKeys(),
                            tableConfig,
                            schema.getTableComment());
            try {
                dorisSystem.createTable(dorisSchema);
            } catch (Exception ex) {
                handleTableCreationFailure(ex, ignoreIncompatible);
            }
        }
    }

    /** Overloaded method without ignoreIncompatible parameter. */
    public static void tryCreateTableIfAbsent(
            DorisSystem dorisSystem,
            String targetDb,
            String dorisTable,
            SourceSchema schema,
            DorisTableConfig tableConfig)
            throws CreateTableException {
        tryCreateTableIfAbsent(dorisSystem, targetDb, dorisTable, schema, tableConfig, false);
    }


    private static void handleTableCreationFailure(Exception ex, boolean ignoreIncompatible)
            throws CreateTableException {
        if (ignoreIncompatible && ex.getCause() instanceof SQLSyntaxErrorException) {
            LOG.warn(
                    "Doris schema and source table schema are not compatible. Error: {} ",
                    ex.getCause().toString());
        } else {
            throw new CreateTableException("Failed to create table due to: ", ex);
        }
    }
}
