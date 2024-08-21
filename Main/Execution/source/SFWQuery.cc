
#ifndef SFW_QUERY_CC
#define SFW_QUERY_CC

#include "ParserTypes.h"
#include <unordered_set>
#include <limits>
#include <algorithm>
#include <regex>

string SFWQuery::createMemoKey(const vector<pair<string, string>>& tables, const vector<ExprTreePtr>& valuesToSelect, const vector<ExprTreePtr>& CNF) {
    string key;
    for (const auto& table : tables) {
        key += table.first + ":" + table.second + ";";
    }
    key += "|";
    for (const auto& expr : valuesToSelect) {
        key += expr->toString() + ";";
    }
    key += "|";
    for (const auto& expr : CNF) {
        key += expr->toString() + ";";
    }
    return key;
}

vector<pair<vector<int>, vector<int>>> SFWQuery::generatePartitions(int n) {
    int count = 1 << n;
    vector<pair<vector<int>, vector<int>>> allSubsets;
    unordered_set<string> uniqueSubsets;

    for (int i = 0; i < count; i++) {
        vector<int> leftSet;
        vector<int> rightSet;

        for (int j = 0; j < n; j++) {
            if (i & (1 << j)) {
                leftSet.push_back(j);
            } else {
                rightSet.push_back(j);
            }
        }

        // Ensure both leftSet and rightSet contain at least one element
        if (!leftSet.empty() && !rightSet.empty()) {
            string leftStr = "", rightStr = "";
            for (int k : leftSet) leftStr += to_string(k) + ",";
            for (int k : rightSet) rightStr += to_string(k) + ",";
            string subsetKey = leftStr + "|" + rightStr;
            string reverseSubsetKey = rightStr + "|" + leftStr;

            // Check if this partition or its reverse has already been added
            if (uniqueSubsets.find(reverseSubsetKey) == uniqueSubsets.end()) {
                uniqueSubsets.insert(subsetKey);
                allSubsets.emplace_back(leftSet, rightSet);
            }
        }
    }

    return allSubsets;
}

pair<LogicalOpPtr, MyDB_SchemaPtr> SFWQuery ::optimize(vector<pair<string, string>> tables, vector<ExprTreePtr> valuesToSelect, vector<ExprTreePtr> CNF) {
	string key = createMemoKey(tables, valuesToSelect, CNF);

	if (memoCache.find(key) != memoCache.end()) {
        return memoCache[key];
    }

	LogicalOpPtr bestOp;
	MyDB_SchemaPtr bestSchema;

	if (tables.size() == 1) {
        string tableName = tables[0].first;
        string aliasName = tables[0].second;
        auto inputTable = allTables[tableName];
        auto inputTableRW = allTableReaderWriters[tableName];

        vector<string> exprs;

        bestSchema = make_shared<MyDB_Schema>();
        for (const auto &expr : valuesToSelect) {
            for (const auto &inputAtt : inputTable->getSchema()->getAtts()){
                if (expr->referencesAtt(aliasName, inputAtt.first)) {
                    bestSchema->getAtts().push_back(make_pair(aliasName + "_" + inputAtt.first, inputAtt.second));
                    exprs.push_back("[" + inputAtt.first + "]");
                }
            }
        }

        bestOp = make_shared<LogicalTableScan>(
            inputTableRW,
            make_shared<MyDB_Table>(aliasName + "_scan", aliasName + "_scan.bin", bestSchema),
            make_shared<MyDB_Stats>(inputTable, aliasName),
            CNF, exprs);

		memoCache[key] = make_pair(bestOp, bestSchema);

        return make_pair(bestOp, bestSchema);
    } else {
		vector<pair<vector<int>, vector<int>>> allPartitions = generatePartitions(tables.size());

		double bestCost = numeric_limits<double>::max();

		for (const auto &partition : allPartitions) {
			vector<pair<string, string>> leftTables;
			vector<pair<string, string>> rightTables;

			for (int i : partition.first) {
				leftTables.push_back(tables[i]);
			}

			for (int i : partition.second) {
				rightTables.push_back(tables[i]);
			}

			MyDB_SchemaPtr leftSchema = make_shared<MyDB_Schema>();
			MyDB_SchemaPtr rightSchema = make_shared<MyDB_Schema>();
			MyDB_SchemaPtr topSchema = make_shared<MyDB_Schema>();

			vector<ExprTreePtr> topExprs;
			vector<ExprTreePtr> leftExprs;
			vector<ExprTreePtr> rightExprs;

			vector<ExprTreePtr> topCNF;
			vector<ExprTreePtr> leftCNF;
			vector<ExprTreePtr> rightCNF;

			for (const auto &expr : CNF) {
				bool inLeft = false;
				bool inRight = false;

				for (const auto &table : leftTables) {
					if (expr->referencesTable(table.second)) {
						inLeft = true;
						break;
					}
				}	

				for (const auto &table : rightTables) {
					if (expr->referencesTable(table.second)) {
						inRight = true;
						break;
					}
				}

				if (inLeft && inRight) topCNF.push_back(expr);
                else if (inLeft) leftCNF.push_back(expr);
                else rightCNF.push_back(expr);
			}
			vector <vector<pair<string, string>>> tables{leftTables, rightTables};

			for (int i = 0; i < 2; i++) {
				for (auto nameAlias : tables[i]) {
					bool isSelect = false;
					bool inCNF = false;

					MyDB_TablePtr table = allTables[nameAlias.first];

					for (auto att : table->getSchema()->getAtts()) {
						for (const auto &expr : valuesToSelect) {
							if (expr->referencesAtt(nameAlias.second, att.first)) {
								isSelect = true;
								break;
							}
						}

						for (const auto &expr : CNF) {
							if (expr->referencesAtt(nameAlias.second, att.first)) {
								inCNF = true;
								break;
							}
						}

						ExprTreePtr idExp = make_shared<Identifier>(strdup(nameAlias.second.c_str()), strdup(att.first.c_str()));

						if (isSelect || inCNF) {
							if (i == 0) {
								leftSchema->getAtts().push_back(make_pair(nameAlias.second + "_" + att.first, att.second));
								leftExprs.push_back(idExp);
							} else if (i == 1) {
								rightSchema->getAtts().push_back(make_pair(nameAlias.second + "_" + att.first, att.second));
								rightExprs.push_back(idExp);
							} 
						}

						if (isSelect) {
							topSchema->getAtts().emplace_back(nameAlias.second + "_" + att.first, att.second);
							topExprs.push_back(idExp);
						}
					}
				}
			}
			
			string leftTableNames = leftTables[0].second;
			for (int i = 1; i < leftTables.size(); ++i) {
				leftTableNames += ("_" + leftTables[i].second);
			}

			string rightTableNames = rightTables[0].second;
			for (int i = 1; i < rightTables.size(); ++i) {
				rightTableNames += ("_" + rightTables[i].second);
			}

			auto leftOptimized = optimize(leftTables, leftExprs, leftCNF);
			auto rightOptimized = optimize(rightTables, rightExprs, rightCNF);

			auto leftOp = leftOptimized.first;
			auto rightOp = rightOptimized.first;
						
			string outputTableName = leftTableNames + "_JOIN_" + rightTableNames;

			LogicalOpPtr myOp;

			myOp = make_shared<LogicalJoin>(
				leftOp, rightOp,
				make_shared<MyDB_Table>(outputTableName, outputTableName + ".bin", topSchema),
				topCNF, topExprs);

			cout << myOp->cost().first << " " << bestCost << endl;
            if (myOp->cost().first < bestCost) {
				cout << "Best schema: " << topSchema << endl;
                bestCost = myOp->cost().first;
                bestOp = myOp;
				bestSchema = topSchema;
            }
		}

    	memoCache[key] = make_pair(bestOp, bestSchema);

		return make_pair(bestOp, bestSchema);
	}
}

// builds and optimizes a logical query plan for a SFW query, returning the logical query plan
// 
// note that this implementation only works for two-table queries that do not have an aggregation
// 
void replace(string &s, string const &toReplace, string const &replaceWith)
{
    ostringstream oss;
    size_t pos = 0;
    size_t prevPos = pos;

    while (true)
    {
        prevPos = pos;
        pos = s.find(toReplace, pos);
        if (pos == string::npos)
            break;
        oss << s.substr(prevPos, pos - prevPos);
        oss << replaceWith;
        pos += toReplace.size();
    }

    oss << s.substr(prevPos);
    s = oss.str();
}

LogicalOpPtr SFWQuery :: buildLogicalQueryPlan (map <string, MyDB_TablePtr> &allTables, map <string, MyDB_TableReaderWriterPtr> &allTableReaderWriters) {
	memoCache.clear();
	
	this->allTables = allTables;
	this->allTableReaderWriters = allTableReaderWriters;

	// also, make sure that there are no aggregates in herre
	bool areAggs = false;

	if (groupingClauses.size () != 0) {
		areAggs = true;
	}
	for (auto a : valuesToSelect) {
		if (a->hasAgg ()) {
			areAggs = true;
		}
	}

    // remove unused tables
    for (auto iter = tablesToProcess.begin(); iter != tablesToProcess.end();)
    {
        auto tableAlias = (*iter).second;
        bool isUsed = any_of(allDisjunctions.begin(), allDisjunctions.end(), [&tableAlias](const ExprTreePtr &expr)
                             { return expr->referencesTable(tableAlias); }) ||
                      any_of(valuesToSelect.begin(), valuesToSelect.end(), [&tableAlias](const ExprTreePtr &expr)
                             { return expr->referencesTable(tableAlias); }) ||
                      any_of(groupingClauses.begin(), groupingClauses.end(), [&tableAlias](const ExprTreePtr &expr)
                             { return expr->referencesTable(tableAlias); });

        if (isUsed)
            ++iter;
        else {
			cout << "Removing unused table: " << (*iter).first << endl;
			tablesToProcess.erase(iter);
		}
    }

	pair<LogicalOpPtr, MyDB_SchemaPtr> best = optimize(tablesToProcess, valuesToSelect, allDisjunctions);	
	LogicalOpPtr myOp = best.first;
	MyDB_SchemaPtr mySchema = best.second;
    MyDB_Record myRecord(mySchema);

	// build the shcema for the output table
	int attId = 0;
	MyDB_SchemaPtr schemaFinal = make_shared <MyDB_Schema> ();
	vector <string> finalAtts;
	for (auto a : valuesToSelect) {
		schemaFinal->getAtts ().push_back (make_pair ("final_" + to_string(attId++), myRecord.getType(a->toString())));
		finalAtts.push_back (a->toString ());
	}

	vector<string> finalSelections;
	vector<ExprTreePtr> aggSelections;
	map <string, string> exprNewName;
	MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();

	for (const auto &expr : groupingClauses) {
		string newName = "Agg_" + to_string(attId++);
		aggSchema->getAtts().emplace_back( newName, myRecord.getType(expr->toString()));
		exprNewName[expr->toString()] = newName;
	}

	// extract identifier from group by clauses
	for (const auto &groupExpr : groupingClauses) {
		for (const auto &idExpr : groupExpr->getIdentifiers()) {
			string idExprStr = idExpr->toString();
			if (exprNewName.find(idExprStr) == exprNewName.end()) {
				string newName = "Agg_" + to_string(attId++);

				groupingClauses.push_back(idExpr);
				aggSchema->getAtts().emplace_back(newName, myRecord.getType(idExprStr));
				exprNewName[idExprStr] = newName;
			}
		}
	}

	// extract aggregate expressions from group by select clauses
	if (areAggs) {
		for (auto &expr : valuesToSelect) {
			for (const auto &aggExpr : expr->getAggExprs()) {
				string aggExprStr = aggExpr->toString();
				if (exprNewName.find(aggExprStr) == exprNewName.end()) {
					string newName = "Agg_" + to_string(attId++);

					aggSelections.push_back(aggExpr);
					aggSchema->getAtts().emplace_back(newName, myRecord.getType(aggExpr->toString()));
					exprNewName[aggExprStr] = newName;
				}
			}
		}
	}

	vector<string> exprOldName;

	for (auto expr : valuesToSelect) {
		string exprStr = expr->toString();
		for (const auto &oldName : exprOldName) {
			replace(exprStr, oldName, "[" + exprNewName[oldName] + "]");
		}
		finalSelections.push_back(exprStr);
	}

	bool allIdentical = true;

	MyDB_SchemaPtr schema = areAggs ? aggSchema : mySchema;

	if (finalSelections.size() == schema->getAtts().size()) {
		for (int i = 0; i < finalSelections.size(); i++) {
			if (finalSelections[i] != "[" + schema->getAtts()[i].first + "]") {
				allIdentical = false;
				break;
			}
		}
	} else allIdentical = false;

	if (!areAggs && allIdentical) return best.first;

	MyDB_SchemaPtr finalSchema = make_shared<MyDB_Schema>();
	MyDB_Record finalRecord(schema);
	LogicalOpPtr finalOpIn = myOp;
	attId = 0;

	if (areAggs) {
		finalOpIn = make_shared<LogicalAggregate>(
            myOp,
            make_shared<MyDB_Table>("agg", "agg.bin", aggSchema),
            aggSelections,
            groupingClauses);

		if (allIdentical) return finalOpIn;
	} 

	for (const auto &exprStr : finalSelections) {
		finalSchema->getAtts().emplace_back("final_" + to_string(attId++), finalRecord.getType(exprStr));
	}

	return make_shared <LogicalTableScan> (
		finalOpIn,
		make_shared <MyDB_Table> ("final", "final.bin", finalSchema),
		finalOpIn->cost().second,
		finalSelections);
}

void SFWQuery :: print () {
	cout << "Selecting the following:\n";
	for (auto a : valuesToSelect) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "From the following:\n";
	for (auto a : tablesToProcess) {
		cout << "\t" << a.first << " AS " << a.second << "\n";
	}
	cout << "Where the following are true:\n";
	for (auto a : allDisjunctions) {
		cout << "\t" << a->toString () << "\n";
	}
	cout << "Group using:\n";
	for (auto a : groupingClauses) {
		cout << "\t" << a->toString () << "\n";
	}
}


SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf, struct ValueList *grouping) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions = cnf->disjunctions;
        groupingClauses = grouping->valuesToCompute;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause,
        struct CNF *cnf) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
		allDisjunctions = cnf->disjunctions;
}

SFWQuery :: SFWQuery (struct ValueList *selectClause, struct FromList *fromClause) {
        valuesToSelect = selectClause->valuesToCompute;
        tablesToProcess = fromClause->aliases;
        allDisjunctions.push_back (make_shared <BoolLiteral> (true));
}

#endif
