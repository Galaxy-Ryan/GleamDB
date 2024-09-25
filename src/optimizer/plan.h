/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "parser/ast.h"

#include "parser/parser.h"
#include "analyze/analyze.h"

typedef enum PlanTag{
    T_Invalid = 1,
    T_Help,
    T_ShowTable,
    T_DescTable,
    T_CreateTable,
    T_DropTable,
    T_ShowIndex,
    T_CreateIndex,
    T_DropIndex,
    T_SetKnob,
    T_SetOutputOff,
    T_CheckPoint,
    T_Load,
    T_Insert,
    T_Update,
    T_Delete,
    T_select,
    T_Transaction_begin,
    T_Transaction_commit,
    T_Transaction_abort,
    T_Transaction_rollback,
    T_SeqScan,
    T_IndexScan,
    T_NestLoop,
    T_SortMerge,    // sort merge join
    T_Sort,
    T_Projection,
    T_Values
} PlanTag;

// 查询执行计划
class Plan
{
public:
    PlanTag tag;
    virtual ~Plan() = default;
};

class ValuesPlan: public Plan {
public:
    ValuesPlan(PlanTag tag, std::vector<Value> values, TabCol col, SmManager* sm_manager) {
        Plan::tag = tag;
        TabMeta tab = sm_manager->db_.get_table(col.tab_name);
        col_ = *tab.get_col(col.col_name);
        values_ = std::move(values);
    }

    PlanTag tag_;
    std::vector<Value> values_;
    ColMeta col_;
};

class ScanPlan : public Plan
{
    public:
        ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, std::vector<std::string> index_col_names)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);
            conds_ = std::move(conds);
            TabMeta &tab = sm_manager->db_.get_table(tab_name_);
            cols_ = tab.cols;
            len_ = cols_.back().offset + cols_.back().len;
            fed_conds_ = conds_;
            index_col_names_ = index_col_names;
            has_subquery_ = false;
        }

        ScanPlan(PlanTag tag, SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                 std::vector<std::string> index_col_names, SubQueryClause sub_clause)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);
            conds_ = std::move(conds);
            TabMeta &tab = sm_manager->db_.get_table(tab_name_);
            cols_ = tab.cols;
            len_ = cols_.back().offset + cols_.back().len;
            fed_conds_ = conds_;
            index_col_names_ = std::move(index_col_names);
            has_subquery_ = true;
            sub_clause_ = std::move(sub_clause);
        }

        ~ScanPlan(){}
        // 以下变量同ScanExecutor中的变量
        std::string tab_name_;                     
        std::vector<ColMeta> cols_;                
        std::vector<Condition> conds_;             
        size_t len_;                               
        std::vector<Condition> fed_conds_;
        std::vector<std::string> index_col_names_;

        bool has_subquery_{false};
        SubQueryClause sub_clause_;
        std::shared_ptr<Plan> sub_plan_;
};

class JoinPlan : public Plan
{
    public:
        JoinPlan(PlanTag tag, std::shared_ptr<Plan> left, std::shared_ptr<Plan> right, std::vector<Condition> conds)
        {
            Plan::tag = tag;
            left_ = std::move(left);
            right_ = std::move(right);
            conds_ = std::move(conds);
            type = INNER_JOIN;
        }
        ~JoinPlan(){}
        // 左节点
        std::shared_ptr<Plan> left_;
        // 右节点
        std::shared_ptr<Plan> right_;
        // 连接条件
        std::vector<Condition> conds_;
        // future TODO: 后续可以支持的连接类型
        JoinType type;
};

class ProjectionPlan : public Plan
{
    public:
        ProjectionPlan(PlanTag tag, std::shared_ptr<Plan> subplan, std::vector<TabCol> sel_cols, std::vector<Alias> aliases)
        {
            Plan::tag = tag;
            subplan_ = std::move(subplan);
            cols_ = std::move(sel_cols);

            for (int i = 0; i < cols_.size(); i++) {
                sel_cols_.push_back(cols_[i]);
                if (aliases[i].has_alias_) {
                    sel_cols_[i].col_name = aliases[i].alias_;
                }
            }
        }
        ~ProjectionPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::vector<TabCol> cols_;
        std::vector<TabCol> sel_cols_;
        
};

class SortPlan : public Plan
{
    public:
        SortPlan(PlanTag tag, std::shared_ptr<Plan> subplan, TabCol sel_col, bool is_desc, SmManager* sm_manager)
        {
            Plan::tag = tag;
            subplan_ = std::move(subplan);
            sel_col_ = sel_col;
            is_desc_ = is_desc;
            sm_manager_ = sm_manager;
        }
        ~SortPlan(){}
        std::shared_ptr<Plan> subplan_;
        TabCol sel_col_;
        bool is_desc_;
        SmManager* sm_manager_;
};

// dml语句，包括insert; delete; update语句　
class DMLPlan : public Plan
{
    public:
        DMLPlan(PlanTag tag, std::shared_ptr<Plan> subplan,std::string tab_name,
                std::vector<Value> values, std::vector<Condition> conds,
                std::vector<SetClause> set_clauses)
        {
            Plan::tag = tag;
            subplan_ = std::move(subplan);
            tab_name_ = std::move(tab_name);
            values_ = std::move(values);
            conds_ = std::move(conds);
            set_clauses_ = std::move(set_clauses);
        }
        ~DMLPlan(){}
        std::shared_ptr<Plan> subplan_;
        std::string tab_name_;
        std::vector<Value> values_;
        std::vector<Condition> conds_;
        std::vector<SetClause> set_clauses_;
};

// dql语句，包括select语句　
class DQLPlan : public Plan
{
public:
    DQLPlan(PlanTag tag, std::shared_ptr<Plan> subplan)
    {
        Plan::tag = tag;
        subplan_ = std::move(subplan);
    }
    ~DQLPlan(){}
    std::shared_ptr<Plan> subplan_;
};

class AggrPlan : public Plan {
public:
    AggrPlan(std::shared_ptr<Plan> subplan, std::vector<HavingCond> havings_,
             std::vector<AggrCol> aggregates, std::vector<TabCol> group_bys)
             : subplan_(std::move(subplan)), havings_(std::move(havings_)), aggregates_(std::move(aggregates)),
               group_bys_(std::move(group_bys)) {}

    std::shared_ptr<Plan> subplan_;
    std::vector<AggrCol> aggregates_;
    std::vector<TabCol> group_bys_;
    std::vector<HavingCond> havings_;
};

// ddl语句, 包括create/drop table; create/drop index;
class DDLPlan : public Plan
{
    public:
        DDLPlan(PlanTag tag, std::string tab_name, std::vector<std::string> col_names, std::vector<ColDef> cols)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);
            cols_ = std::move(cols);
            tab_col_names_ = std::move(col_names);
        }
        ~DDLPlan(){}
        std::string tab_name_;
        std::vector<std::string> tab_col_names_;
        std::vector<ColDef> cols_;
};

// help; show tables; desc tables; begin; abort; commit; rollback语句对应的plan
class OtherPlan : public Plan
{
    public:
        OtherPlan(PlanTag tag, std::string tab_name)
        {
            Plan::tag = tag;
            tab_name_ = std::move(tab_name);            
        }
        ~OtherPlan(){}
        std::string tab_name_;
};

// Set Knob Plan
class SetKnobPlan : public Plan
{
    public:
        SetKnobPlan(ast::SetKnobType knob_type, bool bool_value) {
            Plan::tag = T_SetKnob;
            set_knob_type_ = knob_type;
            bool_value_ = bool_value;
        }
    ast::SetKnobType set_knob_type_;
    bool bool_value_;
};

class SetOutputOffPlan : public Plan
{
    public:
        SetOutputOffPlan() {
            Plan::tag = T_SetOutputOff;
        }
};

class CkpPlan : public Plan {
public:
    CkpPlan() {
        Plan::tag = T_CheckPoint;
    }
};

class LoadPlan : public Plan {
public:
    LoadPlan(std::string path, std::string tab_name) {
        Plan::tag = T_Load;
        path_ = std::move(path);
        tab_name_ = std::move(tab_name);
    }
    std::string path_;
    std::string tab_name_;
};

class plannerInfo{
    public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;
    std::vector<SetClause> set_clauses;
    plannerInfo(std::shared_ptr<ast::SelectStmt> parse_):parse(std::move(parse_)){}

};


class aggrInfo {
public:
    std::shared_ptr<ast::SelectStmt> parse;
    std::vector<Condition> where_conds;
    std::vector<TabCol> sel_cols;
    std::shared_ptr<Plan> plan;
    std::vector<std::shared_ptr<Plan>> table_scan_executors;

    std::vector<TabCol> group_bys_;
    std::vector<Condition> havings_;
    aggrInfo(std::shared_ptr<ast::SelectStmt> parse_):parse(std::move(parse_)){}
};
