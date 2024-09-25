/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "analyze.h"

/**
 * @description: 分析器，进行语义分析和查询重写，需要检查不符合语义规定的部分
 * @param {shared_ptr<ast::TreeNode>} parse parser生成的结果集
 * @return {shared_ptr<Query>} Query 
 */
std::shared_ptr<Query> Analyze::do_analyze(std::shared_ptr<ast::TreeNode> parse)
{
    std::shared_ptr<Query> query = std::make_shared<Query>();
    if (auto x = std::dynamic_pointer_cast<ast::UpdateStmt>(parse)) {
        /** TODO: */
        for (auto clause: x->set_clauses) {
            TabCol tab_col{x->tab_name, clause->col_name};
            Value new_val = convert_sv_value(clause->val);
            SetClause new_clause{tab_col, new_val};
            query->set_clauses.emplace_back(new_clause);
        }
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);
    } else if (auto x = std::dynamic_pointer_cast<ast::DeleteStmt>(parse)) {
        //处理where条件
        get_clause(x->conds, query->conds);
        check_clause({x->tab_name}, query->conds);        
    } else if (auto x = std::dynamic_pointer_cast<ast::InsertStmt>(parse)) {
        // 处理insert 的values值
        for (auto &sv_val : x->vals) {
            query->values.push_back(convert_sv_value(sv_val));
        }
    } else if (auto x = std::dynamic_pointer_cast<ast::SelectStmt>(parse)) {
        analyze_one_select(x.get(), query);
    } else {
        // do nothing
    }
    query->parse = std::move(parse);
    return query;
}


TabCol Analyze::check_column(const std::vector<ColMeta> &all_cols, TabCol target) {
    if (target.tab_name.empty()) {
        // Table name not specified, infer table name from column name
        std::string tab_name;
        for (auto &col : all_cols) {
            if (col.name == target.col_name) {
                if (!tab_name.empty()) {
                    throw AmbiguousColumnError(target.col_name);
                }
                tab_name = col.tab_name;
            }
        }
        if (tab_name.empty()) {
            throw ColumnNotFoundError(target.col_name);
        }
        target.tab_name = tab_name;
    } else {
        /** TODO: Make sure target column exists */

    }
    return target;
}

void Analyze::get_all_cols(const std::vector<std::string> &tab_names, std::vector<ColMeta> &all_cols) {
    for (auto &sel_tab_name : tab_names) {
        // 这里db_不能写成get_db(), 注意要传指针
        const auto &sel_tab_cols = sm_manager_->db_.get_table(sel_tab_name).cols;
        all_cols.insert(all_cols.end(), sel_tab_cols.begin(), sel_tab_cols.end());
    }
}

void Analyze::get_clause(const std::vector<std::shared_ptr<ast::BinaryExpr>> &sv_conds, std::vector<Condition> &conds) {
    conds.clear();
    for (auto &expr : sv_conds) {
        Condition cond;
        cond.lhs_col = {.tab_name = expr->lhs->tab_name, .col_name = expr->lhs->col_name};
        cond.op = convert_sv_comp_op(expr->op);
        if (auto rhs_val = std::dynamic_pointer_cast<ast::Value>(expr->rhs)) {
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(rhs_val);
        } else if (auto rhs_col = std::dynamic_pointer_cast<ast::Col>(expr->rhs)) {
            cond.is_rhs_val = false;
            cond.rhs_col = {.tab_name = rhs_col->tab_name, .col_name = rhs_col->col_name};
        } else {
            continue;
        }
        conds.push_back(cond);
    }
}

void Analyze::check_clause(const std::vector<std::string> &tab_names, std::vector<Condition> &conds) {
    // auto all_cols = get_all_cols(tab_names);
    std::vector<ColMeta> all_cols;
    get_all_cols(tab_names, all_cols);
    // Get raw values in where clause
    for (auto &cond : conds) {
        // Infer table name from column name
        cond.lhs_col = check_column(all_cols, cond.lhs_col);
        if (!cond.is_rhs_val) {
            cond.rhs_col = check_column(all_cols, cond.rhs_col);
        }
        TabMeta &lhs_tab = sm_manager_->db_.get_table(cond.lhs_col.tab_name);
        auto lhs_col = lhs_tab.get_col(cond.lhs_col.col_name);
        ColType lhs_type = lhs_col->type;
        ColType rhs_type;
        if (cond.is_rhs_val) {
            cond.rhs_val.init_raw(lhs_col->len);
            rhs_type = cond.rhs_val.type;
        } else {
            TabMeta &rhs_tab = sm_manager_->db_.get_table(cond.rhs_col.tab_name);
            auto rhs_col = rhs_tab.get_col(cond.rhs_col.col_name);
            rhs_type = rhs_col->type;
        }
        if (!Value::TypeCompatible(lhs_type, rhs_type)) {
            throw IncompatibleTypeError("", "");
        }
    }
}

void Analyze::check_aggr_cols(const std::vector<TabCol> &cols, const std::vector<TabCol> &group_bys) {
    for (auto& col: cols) {
        auto it = std::find(group_bys.begin(), group_bys.end(), col);
        if (it == group_bys.end()) {
            throw SelectNonGroupByError();
        }
    }
}

void Analyze::check_tables(std::vector<std::string> tabs) {
    for (auto& tab: tabs) {
        if (sm_manager_->fhs_.find(tab) == sm_manager_->fhs_.end()) {
            throw TableNotFoundError(tab);
        }
    }
}

Value Analyze::convert_sv_value(const std::shared_ptr<ast::Value> &sv_val) {
    Value val;
    if (auto int_lit = std::dynamic_pointer_cast<ast::IntLit>(sv_val)) {
        val.set_int(int_lit->val);
    } else if (auto float_lit = std::dynamic_pointer_cast<ast::FloatLit>(sv_val)) {
        val.set_float(float_lit->val);
    } else if (auto str_lit = std::dynamic_pointer_cast<ast::StringLit>(sv_val)) {
        val.set_str(str_lit->val);
    } else {
        throw InternalError("Unexpected sv value type");
    }
    return val;
}

CompOp Analyze::convert_sv_comp_op(ast::SvCompOp op) {
    std::map<ast::SvCompOp, CompOp> m = {
        {ast::SV_OP_EQ, OP_EQ}, {ast::SV_OP_NE, OP_NE}, {ast::SV_OP_LT, OP_LT},
        {ast::SV_OP_GT, OP_GT}, {ast::SV_OP_LE, OP_LE}, {ast::SV_OP_GE, OP_GE},
    };
    return m.at(op);
}

AggrType Analyze::convert_sv_aggr(ast::AggrType aggr_type) {
    switch (aggr_type) {
        case ast::AGG_COUNT:
            return COUNT;
        case ast::AGG_MAX:
            return MAX;
        case ast::AGG_MIN:
            return MIN;
        case ast::AGG_SUM:
            return SUM;
        case ast::AGG_COUNT_STAR:
            return COUNT_STAR;
        case ast::AGG_NO_OP:
            return NON_AGG;
        default:
            break;
    }
}

void Analyze::analyze_one_select(ast::SelectStmt* x, std::shared_ptr<Query> query) {
    query->tables = std::move(x->tabs_);
    check_tables(query->tables);

    std::vector<ColMeta> all_cols;
    get_all_cols(query->tables, all_cols);
    std::vector<TabCol> non_agg_cols;

    for (auto &col: x->agg_cols_) {
        if (col->agg_type_ == ast::NON_AGG_ALL) {
            for (auto &allCol : all_cols) {
                TabCol sel_col = {.tab_name = allCol.tab_name, .col_name = allCol.name};
                query->aliases_.push_back({.has_alias_ = false, .alias_ = ""});
                query->cols.push_back(sel_col);
                non_agg_cols.push_back(sel_col);
            }
        } else if (col->agg_type_ == ast::AGG_NO_OP) {
            TabCol sel_col = {.tab_name = col->column_->tab_name, .col_name = col->column_->col_name};
            sel_col = check_column(all_cols, sel_col);
            query->cols.push_back(sel_col);
            if (col->has_alias_) {
                query->aliases_.push_back({.has_alias_ = true, .alias_ = col->alias_});
            } else {
                query->aliases_.push_back({.has_alias_ = false, .alias_ = ""});
            }
            non_agg_cols.push_back(sel_col);
        } else if (col->agg_type_ == ast::AGG_COUNT_STAR) {
            AggrCol aggr_col;
            aggr_col.alias_ = col->alias_;
            aggr_col.type_ = convert_sv_aggr(col->agg_type_);
            query->cols.push_back({"", aggr_col.alias_});
            query->aliases_.push_back({.has_alias_ = false, .alias_ = ""});
            query->aggregates_.push_back(aggr_col);
        } else {
            AggrCol aggr_col;
            aggr_col.alias_ = col->alias_;
            aggr_col.tab_col_ = {.tab_name = col->column_->tab_name, .col_name = col->column_->col_name};
            aggr_col.tab_col_ = check_column(all_cols, aggr_col.tab_col_);
            check_column(all_cols, aggr_col.tab_col_);
            aggr_col.type_ = convert_sv_aggr(col->agg_type_);
            query->cols.push_back({col->column_->tab_name, aggr_col.alias_});
            query->aliases_.push_back({.has_alias_ = false, .alias_ = ""});
            query->aggregates_.push_back(aggr_col);
        }
    }

    //处理where条件
    get_clause(x->conds_, query->conds);
    check_clause(query->tables, query->conds);

    //处理subquery
    query->has_subquery_ = false;
    for (auto& cond: x->conds_) {
        if (auto tmp = std::dynamic_pointer_cast<ast::Subquery>(cond->rhs)) {
            auto subquery = std::dynamic_pointer_cast<ast::SelectStmt>(tmp->sub_query_);
            if (subquery != nullptr) {
                query->has_subquery_ = true;
                if (cond->in_clause) {
                    query->subquery_.in_clause = true;
                } else {
                    query->subquery_.op = convert_sv_comp_op(cond->op);
                }
                query->subquery_.lhs = {
                        .tab_name = cond->lhs->tab_name,
                        .col_name = cond->lhs->col_name};
                query->subquery_.lhs = check_column(all_cols, query->subquery_.lhs);
                query->subquery_.sub = std::make_shared<Query>();
                analyze_one_select(subquery.get(), query->subquery_.sub);
            }
        } else if (auto valueList = std::dynamic_pointer_cast<ast::ValueList>(cond->rhs)){
            query->has_subquery_ = true;
            if (cond->in_clause) {
                query->subquery_.in_clause = true;
            } else if (valueList->values_.size() != 1){
                throw InternalError("valueList more than 1");
            }
            query->subquery_.lhs = {
                    .tab_name = cond->lhs->tab_name,
                    .col_name = cond->lhs->col_name};
            query->subquery_.lhs = check_column(all_cols, query->subquery_.lhs);
            query->subquery_.sub = std::make_shared<Query>();
            query->subquery_.sub->cols.push_back(query->subquery_.lhs);
            for (auto& value: valueList->values_) {
                query->subquery_.sub->values.push_back(convert_sv_value(value));
            }
            query->subquery_.value_list = true;
        }
    }

    //处理group by clause
    for (auto &col : x->group_bys_) {
        TabCol sel_col = {.tab_name = col->tab_name, .col_name = col->col_name};
        sel_col = check_column(all_cols, sel_col);
        query->group_bys_.push_back(sel_col);
    }

    // 检查列表中是否有出现没有在 GROUP BY 子句中的非聚集列
    if (!x->group_bys_.empty()) {
        check_aggr_cols(non_agg_cols, query->group_bys_);
    }

    //处理having clause
    for (auto &having : x->havings_) {
        auto lhs = having->lhs_;
        if (lhs->agg_type_ == ast::AGG_COUNT_STAR) {
            HavingCond cond;
            AggrCol aggr_col;
            aggr_col.type_ = convert_sv_aggr(lhs->agg_type_);
            cond.lhs_col = aggr_col;
            cond.op = convert_sv_comp_op(having->op_);
            cond.rhs_val = convert_sv_value(having->rhs_);
            query->havings_.push_back(cond);
        } else if (lhs->agg_type_ == ast::AGG_NO_OP) {
            Condition cond;
            TabCol tab_col = {.tab_name = lhs->column_->tab_name, .col_name = lhs->column_->col_name};
            cond.lhs_col = check_column(all_cols, tab_col);
            if (std::find(query->group_bys_.begin(), query->group_bys_.end(), cond.lhs_col) == query->group_bys_.end()) {
                throw InternalError("column must appear in the GROUP BY clause or be used in an aggregate function");
            }
            cond.op = convert_sv_comp_op(having->op_);
            cond.is_rhs_val = true;
            cond.rhs_val = convert_sv_value(having->rhs_);
            query->conds.push_back(cond);
        } else {
            HavingCond cond;
            AggrCol aggr_col;
            aggr_col.tab_col_ = {.tab_name = lhs->column_->tab_name, .col_name = lhs->column_->col_name};
            aggr_col.tab_col_ = check_column(all_cols, aggr_col.tab_col_);
            check_column(all_cols, aggr_col.tab_col_);
            aggr_col.type_ = convert_sv_aggr(lhs->agg_type_);
            cond.lhs_col = aggr_col;
            cond.op = convert_sv_comp_op(having->op_);
            cond.rhs_val = convert_sv_value(having->rhs_);
            query->havings_.push_back(cond);
        }
    }
}
