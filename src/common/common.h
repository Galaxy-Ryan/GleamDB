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
#include <vector>
#include <cfloat>
#include "defs.h"
#include "record/rm_defs.h"


struct TabCol {
    std::string tab_name;
    std::string col_name;

    friend bool operator<(const TabCol &x, const TabCol &y) {
        return std::make_pair(x.tab_name, x.col_name) < std::make_pair(y.tab_name, y.col_name);
    }

    friend bool operator==(const TabCol &x, const TabCol &y) {
        return x.tab_name == y.tab_name && x.col_name == y.col_name;
    }
};

enum AggrType { SUM, MAX, MIN, COUNT, COUNT_STAR, NON_AGG };

struct AggrCol {
    TabCol tab_col_;
    AggrType type_;
    std::string alias_;
};

enum CompOp { OP_EQ, OP_NE, OP_LT, OP_GT, OP_LE, OP_GE };

struct Value {
    ColType type;  // type of value
    union {
        int int_val;      // int value
        float float_val;  // float value
    };
    std::string str_val;  // string value

    std::shared_ptr<RmRecord> raw;  // raw record buffer

    void set_int(int int_val_) {
        type = TYPE_INT;
        int_val = int_val_;
    }

    void set_float(float float_val_) {
        type = TYPE_FLOAT;
        float_val = float_val_;
    }

    void set_str(std::string str_val_) {
        type = TYPE_STRING;
        str_val = std::move(str_val_);
    }

    void set_datetime(std::string val_) {
        type = TYPE_STRING;
        str_val = std::move(val_);
    }

    void init_raw(int len) {
        assert(raw == nullptr);
        raw = std::make_shared<RmRecord>(len);
        if (type == TYPE_INT) {
            assert(len == sizeof(int));
            *(int *)(raw->data) = int_val;
        } else if (type == TYPE_FLOAT) {
            assert(len == sizeof(float));
            *(float *)(raw->data) = float_val;
        } else if (type == TYPE_STRING) {
            if (len < (int)str_val.size()) {
                throw StringOverflowError();
            }
            memset(raw->data, 0, len);
            memcpy(raw->data, str_val.c_str(), str_val.size());
        }
    }

    void generate_max(ColType type, int len) {
        this->type = type;
        switch (type) {
            case TYPE_INT:
                int_val = INT32_MAX;
                init_raw(len);
                break;
            case TYPE_FLOAT:
                float_val = FLT_MAX;
                init_raw(len);
                break;
            case TYPE_STRING:
                str_val.resize(len, 126); // 最大的可打印字符
                raw = std::make_shared<RmRecord>(len);
                memset(raw->data, 126, len);
                break;
            default:
                break;
        }
    }

    void generate_min(ColType type, int len) {
        this->type = type;
        switch (type) {
            case TYPE_INT:
                int_val = INT32_MIN;
                init_raw(len);
                break;
            case TYPE_FLOAT:
                float_val = FLT_MIN;
                init_raw(len);
                break;
            case TYPE_STRING:
                str_val = "";
                raw = std::make_shared<RmRecord>(len);
                memset(raw->data, 0, len);
                break;
            default:
                break;
        }
    }

    inline static bool ValueComp(Value left, Value right, CompOp op) {
        if (left.type == TYPE_INT && right.type == TYPE_INT) {
            return ValueComp(left.int_val, right.int_val, op);
        } else if (left.type == TYPE_FLOAT && right.type == TYPE_FLOAT) {
            return ValueComp(left.float_val, right.float_val, op);
        } else if (left.type == TYPE_STRING && right.type == TYPE_STRING) {
            return ValueComp(left.str_val, right.str_val, op);
        } else if (left.type == TYPE_FLOAT && right.type == TYPE_INT) {
            return ValueComp(left.float_val, static_cast<float>(right.int_val), op);
        } else if (left.type == TYPE_INT && right.type == TYPE_FLOAT) {
            return ValueComp(static_cast<float>(left.int_val), right.float_val, op);
        }else {
            throw IncompatibleTypeError("", "");
        }
    }

    inline static bool ValueComp(int left, int right, CompOp op) {
        switch (op) {
            case OP_EQ: return left == right;
            case OP_NE: return left != right;
            case OP_GE: return left >= right;
            case OP_LE: return left <= right;
            case OP_GT: return left > right;
            case OP_LT: return left < right;
            default: break;
        }
        return false;
    }

    inline static bool ValueComp(float left, float right, CompOp op) {
        switch (op) {
            case OP_EQ: return left == right;
            case OP_NE: return left != right;
            case OP_GE: return left >= right;
            case OP_LE: return left <= right;
            case OP_GT: return left > right;
            case OP_LT: return left < right;
            default: break;
        }
        return false;
    }

    inline static bool ValueComp(std::string left, std::string right, CompOp op) {
        switch (op) {
            case OP_EQ: return left == right;
            case OP_NE: return left != right;
            case OP_GE: return left >= right;
            case OP_LE: return left <= right;
            case OP_GT: return left > right;
            case OP_LT: return left < right;
            default: break;
        }
        return false;
    }

    static auto TypeCompatible(ColType left, ColType right) -> bool {
        if (left == right) {
            return true;
        }
        if (left == TYPE_INT && right == TYPE_FLOAT) {
            return true;
        }
        if (left == TYPE_FLOAT && right == TYPE_INT) {
            return true;
        }
        return false;
    }
};

struct Condition {
    TabCol lhs_col;   // left-hand side column
    CompOp op;        // comparison operator
    bool is_rhs_val;  // true if right-hand side is a value (not a column)
    TabCol rhs_col;   // right-hand side column
    Value rhs_val;    // right-hand side value
};

struct HavingCond {
    AggrCol lhs_col;
    CompOp op;
    Value rhs_val;
};

struct SetClause {
    TabCol lhs;
    Value rhs;
};

struct SubQueryClause {
    TabCol lhs;
    CompOp op;
    bool in_clause;
};