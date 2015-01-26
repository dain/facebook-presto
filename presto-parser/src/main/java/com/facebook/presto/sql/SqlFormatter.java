/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AssignmentStatement;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.CallStatement;
import com.facebook.presto.sql.tree.CaseStatement;
import com.facebook.presto.sql.tree.CaseStatementWhenClause;
import com.facebook.presto.sql.tree.CompoundStatement;
import com.facebook.presto.sql.tree.CreateFunction;
import com.facebook.presto.sql.tree.CreateProcedure;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.CreateTableAsSelect;
import com.facebook.presto.sql.tree.CreateView;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.DropView;
import com.facebook.presto.sql.tree.ElseClause;
import com.facebook.presto.sql.tree.ElseIfClause;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.IfStatement;
import com.facebook.presto.sql.tree.Insert;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.IterateStatement;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.LeaveStatement;
import com.facebook.presto.sql.tree.LoopStatement;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.ParameterDeclaration;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.RenameTable;
import com.facebook.presto.sql.tree.RepeatStatement;
import com.facebook.presto.sql.tree.ResetSession;
import com.facebook.presto.sql.tree.ReturnClause;
import com.facebook.presto.sql.tree.ReturnStatement;
import com.facebook.presto.sql.tree.RoutineCharacteristics;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SetSession;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowSession;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Unnest;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.VariableDeclaration;
import com.facebook.presto.sql.tree.WhileStatement;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionFormatter.formatSortItems;
import static com.facebook.presto.sql.ExpressionFormatter.formatStringLiteral;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;

public final class SqlFormatter
{
    private static final String INDENT = "   ";

    private SqlFormatter() {}

    public static String formatSql(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static class Formatter
            extends AstVisitor<Void, Integer>
    {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitUnnest(Unnest node, Integer indent)
        {
            builder.append(node.toString());
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, query.getName());
                    appendAliasColumns(builder, query.getColumnNames());
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), indent);

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + formatSortItems(node.getOrderBy()))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            if (node.getApproximate().isPresent()) {
                String confidence = node.getApproximate().get().getConfidence();
                append(indent, "APPROXIMATE AT " + confidence + " CONFIDENCE")
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(transform(node.getGroupBy(), ExpressionFormatter::formatExpression)))
                        .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + formatSortItems(node.getOrderBy()))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }
            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }
            if (!node.getTargets().isEmpty()) {
                builder.append(" INTO ");
                Joiner.on(", ").appendTo(builder, node.getTargets());
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression()));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append('"')
                        .append(node.getAlias().get())
                        .append('"'); // TODO: handle quoting properly
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context)
        {
            builder.append(node.toString());

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(node.getName().toString());
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orElse(null);
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append('(');
            }
            process(node.getLeft(), indent);

            builder.append('\n');
            if (node.getType() == Join.Type.IMPLICIT) {
                append(indent, ", ");
            }
            else {
                append(indent, type).append(" JOIN ");
            }

            process(node.getRight(), indent);

            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                if (criteria instanceof JoinUsing) {
                    JoinUsing using = (JoinUsing) criteria;
                    builder.append(" USING (")
                            .append(Joiner.on(", ").join(using.getColumns()))
                            .append(")");
                }
                else if (criteria instanceof JoinOn) {
                    JoinOn on = (JoinOn) criteria;
                    builder.append(" ON (")
                            .append(formatExpression(on.getExpression()))
                            .append(")");
                }
                else if (!(criteria instanceof NaturalJoin)) {
                    throw new UnsupportedOperationException("unknown join criteria: " + criteria);
                }
            }

            if (node.getType() != Join.Type.IMPLICIT) {
                builder.append(")");
            }

            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(node.getAlias());

            appendAliasColumns(builder, node.getColumnNames());

            return null;
        }

        @Override
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            if (node.getColumnsToStratifyOn().isPresent()) {
                builder.append(" STRATIFY ON ")
                        .append(" (")
                        .append(Joiner.on(",").join(node.getColumnsToStratifyOn().get()));
                builder.append(')');
            }

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Expression row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                builder.append(formatExpression(row));
                first = false;
            }

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitCreateView(CreateView node, Integer indent)
        {
            builder.append("CREATE ");
            if (node.isReplace()) {
                builder.append("OR REPLACE ");
            }
            builder.append("VIEW ")
                    .append(node.getName())
                    .append(" AS\n");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitDropView(DropView node, Integer context)
        {
            builder.append("DROP VIEW ")
                    .append(node.getName());

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                }
                else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context)
        {
            builder.append("SHOW CATALOGS");

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context)
        {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context)
        {
            builder.append("SHOW TABLES");

            node.getSchema().ifPresent((value) ->
                    builder.append(" FROM ")
                            .append(value));

            node.getLikePattern().ifPresent((value) ->
                    builder.append(" LIKE ")
                            .append(formatStringLiteral(value)));

            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(node.getTable());

            return null;
        }

        @Override
        protected Void visitShowPartitions(ShowPartitions node, Integer context)
        {
            builder.append("SHOW PARTITIONS FROM ")
                    .append(node.getTable());

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get()));
            }

            if (!node.getOrderBy().isEmpty()) {
                builder.append(" ORDER BY ")
                        .append(formatSortItems(node.getOrderBy()));
            }

            if (node.getLimit().isPresent()) {
                builder.append(" LIMIT ")
                        .append(node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context)
        {
            builder.append("SHOW FUNCTIONS");

            return null;
        }

        @Override
        protected Void visitShowSession(ShowSession node, Integer context)
        {
            builder.append("SHOW SESSION");

            return null;
        }

        @Override
        protected Void visitCreateTableAsSelect(CreateTableAsSelect node, Integer indent)
        {
            builder.append("CREATE TABLE ")
                    .append(node.getName())
                    .append(" AS ");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE TABLE ")
                    .append(node.getName())
                    .append(" (");

            Joiner.on(", ").appendTo(builder, transform(node.getElements(),
                    element -> element.getName() + " " + element.getType()));

            builder.append(")");
            return null;
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer context)
        {
            builder.append("DROP TABLE ")
                    .append(node.getTableName());

            return null;
        }

        @Override
        protected Void visitRenameTable(RenameTable node, Integer context)
        {
            builder.append("ALTER TABLE ")
                    .append(node.getSource())
                    .append(" RENAME TO ")
                    .append(node.getTarget());

            return null;
        }

        @Override
        protected Void visitInsert(Insert node, Integer indent)
        {
            builder.append("INSERT INTO ")
                    .append(node.getTarget())
                    .append(" ");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        public Void visitSetSession(SetSession node, Integer context)
        {
            builder.append("SET SESSION ")
                    .append(node.getName())
                    .append(" = ")
                    .append(formatStringLiteral(node.getValue()));

            return null;
        }

        @Override
        public Void visitResetSession(ResetSession node, Integer context)
        {
            builder.append("RESET SESSION ")
                    .append(node.getName());

            return null;
        }

        @Override
        protected Void visitCreateFunction(CreateFunction node, Integer indent)
        {
            builder.append("CREATE FUNCTION ")
                    .append(node.getName())
                    .append("(");
            processParameters(node.getParameters(), indent);
            builder.append(")\n");
            process(node.getReturnClause(), indent);
            builder.append("\n");
            process(node.getRoutineCharacteristics(), indent);
            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitCreateProcedure(CreateProcedure node, Integer indent)
        {
            builder.append("CREATE PROCEDURE ")
                    .append(node.getName())
                    .append("(");
            processParameters(node.getParameters(), indent);
            builder.append(")\n");
            process(node.getRoutineCharacteristics(), indent);
            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitParameterDeclaration(ParameterDeclaration node, Integer indent)
        {
            if (node.getMode().isPresent()) {
                builder.append(node.getMode().get())
                        .append(" ");
            }
            if (node.getName().isPresent()) {
                builder.append(node.getName().get())
                        .append(" ");
            }
            builder.append(node.getType());
            if (node.getDefaultValue().isPresent()) {
                builder.append(" DEFAULT");
                process(node.getDefaultValue().get(), indent);
            }
            return null;
        }

        @Override
        protected Void visitRoutineCharacteristics(RoutineCharacteristics node, Integer indent)
        {
            for (QualifiedName qualifiedName : node.getSpecificCharacteristics()) {
                builder.append("SPECIFIC ")
                        .append(qualifiedName)
                        .append("\n");
            }
            if (node.isDeterministic().isPresent()) {
                if (!node.isDeterministic().get()) {
                    builder.append("NOT ");
                }
                builder.append("DETERMINISTIC\n");
            }
            if (node.getSqlDataAccessType().isPresent()) {
                switch (node.getSqlDataAccessType().get()) {
                    case NO_SQL:
                        builder.append("NO SQL\n");
                        break;
                    case CONTAINS_SQL:
                        builder.append("CONTAINS SQL\n");
                        break;
                    case READS_SQL_DATA:
                        builder.append("READS SQL DATA\n");
                        break;
                    case MODIFIES_SQL_DATA:
                        builder.append("MODIFIES SQL DATA\n");
                        break;
                }
            }
            if (node.isReturnsNullOnNullInput().isPresent()) {
                if (node.isReturnsNullOnNullInput().get()) {
                    builder.append("RETURNS NULL ON NULL INPUT\n");
                }
                else {
                    builder.append("CALLED ON NULL INPUT\n");
                }
            }
            if (node.getDynamicResultSets().isPresent()) {
                builder.append("DYNAMIC RESULT SETS ")
                        .append(node.getDynamicResultSets())
                        .append("\n");
            }

            return null;
        }

        @Override
        protected Void visitReturnClause(ReturnClause node, Integer indent)
        {
            builder.append("RETURNS ")
                    .append(node.getReturnType());
            if (node.getCastFromType().isPresent()) {
                builder.append(" CAST FROM ")
                        .append(node.getCastFromType().get());
            }
            return null;
        }

        @Override
        protected Void visitCallStatement(CallStatement node, Integer indent)
        {
            builder.append("CALL ")
                    .append(node.getName());
            return null;
        }

        @Override
        protected Void visitReturnStatement(ReturnStatement node, Integer context)
        {
            builder.append("RETURN ")
                    .append(node.getValue());
            return null;
        }

        @Override
        protected Void visitCompoundStatement(CompoundStatement node, Integer indent)
        {
            appendBeginLabel(node.getLabel());
            builder.append("BEGIN\n");
            for (VariableDeclaration variableDeclaration : node.getVariableDeclarations()) {
                builder.append(indentString(indent + 1));
                process(variableDeclaration, indent + 1);
                builder.append(";\n");
            }
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END");
            if (node.getLabel().isPresent()) {
                builder.append(" ")
                        .append(node.getLabel().get());
            }
            return null;
        }

        @Override
        protected Void visitVariableDeclaration(VariableDeclaration node, Integer indent)
        {
            builder.append("DECLARE ");
            Joiner.on(", ").appendTo(builder, node.getNames());
            builder.append(" ")
                    .append(node.getType());
            if (node.getDefaultValue().isPresent()) {
                builder.append(" DEFAULT ")
                        .append(formatExpression(node.getDefaultValue().get()));
            }
            return null;
        }

        @Override
        protected Void visitAssignmentStatement(AssignmentStatement node, Integer indent)
        {
            builder.append("SET ");
            if (node.getTargets().size() == 1) {
                builder.append(getOnlyElement(node.getTargets()));
            }
            else {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, node.getTargets());
                builder.append(")");
            }
            builder.append(" = ")
                    .append(formatExpression(node.getValue()));
            return null;
        }

        @Override
        protected Void visitCaseStatement(CaseStatement node, Integer indent)
        {
            builder.append("CASE");
            if (node.getExpression().isPresent()) {
                builder.append(" ")
                        .append(formatExpression(node.getExpression().get()));
            }
            builder.append("\n");
            for (CaseStatementWhenClause whenClause : node.getWhenClauses()) {
                builder.append(indentString(indent + 1));
                process(whenClause, indent + 1);
            }
            if (node.getElseClause().isPresent()) {
                builder.append(indentString(indent + 1));
                process(node.getElseClause().get(), indent + 1);
            }
            append(indent, "END CASE");
            return null;
        }

        @Override
        protected Void visitCaseStatementWhenClause(CaseStatementWhenClause node, Integer indent)
        {
            builder.append("WHEN ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            return null;
        }

        @Override
        protected Void visitIfStatement(IfStatement node, Integer indent)
        {
            builder.append("IF ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            for (ElseIfClause elseIfClause : node.getElseIfClauses()) {
                builder.append(indentString(indent));
                process(elseIfClause, indent);
            }
            if (node.getElseClause().isPresent()) {
                builder.append(indentString(indent));
                process(node.getElseClause().get(), indent);
            }
            append(indent, "END IF");
            return null;
        }

        @Override
        protected Void visitElseIfClause(ElseIfClause node, Integer indent)
        {
            builder.append("ELSIF ")
                    .append(formatExpression(node.getExpression()))
                    .append(" THEN\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            return null;
        }

        @Override
        protected Void visitElseClause(ElseClause node, Integer indent)
        {
            builder.append("ELSE\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            return null;
        }

        @Override
        protected Void visitIterateStatement(IterateStatement node, Integer indent)
        {
            builder.append("ITERATE ")
                    .append(node.getLabel());
            return null;
        }

        @Override
        protected Void visitLeaveStatement(LeaveStatement node, Integer indent)
        {
            builder.append("LEAVE ")
                    .append(node.getLabel());
            return null;
        }

        @Override
        protected Void visitLoopStatement(LoopStatement node, Integer indent)
        {
            appendBeginLabel(node.getLabel());
            builder.append("LOOP\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END LOOP");
            return null;
        }

        @Override
        protected Void visitWhileStatement(WhileStatement node, Integer indent)
        {
            appendBeginLabel(node.getLabel());
            builder.append("WHILE ")
                    .append(formatExpression(node.getExpression()))
                    .append(" DO\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "END WHILE");
            return null;
        }

        @Override
        protected Void visitRepeatStatement(RepeatStatement node, Integer indent)
        {
            appendBeginLabel(node.getLabel());
            builder.append("REPEAT\n");
            for (Statement statement : node.getStatements()) {
                builder.append(indentString(indent + 1));
                process(statement, indent + 1);
                builder.append(";\n");
            }
            append(indent, "UNTIL ")
                    .append(formatExpression(node.getCondition()))
                    .append("\n");
            append(indent, "END REPEAT");
            return null;
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
        }

        private void processParameters(List<ParameterDeclaration> parameters, Integer indent)
        {
            Iterator<ParameterDeclaration> iterator = parameters.iterator();
            while (iterator.hasNext()) {
                process(iterator.next(), indent);
                if (iterator.hasNext()) {
                    builder.append(", ");
                }
            }
        }

        private void appendBeginLabel(Optional<String> label)
        {
            if (label.isPresent()) {
                builder.append(label.get())
                        .append(": ");
            }
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
        }
    }

    private static void appendAliasColumns(StringBuilder builder, List<String> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, columns);
            builder.append(')');
        }
    }
}
