//
// Created by Pepephant on 2024/6/27.
//

class ValuesExecutor : public AbstractExecutor {
public:
    ValuesExecutor(std::vector<Value> values, ColMeta col, Context* context) {
        values_ = std::move(values);
        tupleLen_ = col.len;
        col.offset = 0;
        cols_.push_back(col);
        context_ = context;
    }

    size_t tupleLen() const override { return tupleLen_; };

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    };

    void beginTuple() override{
        values_it_ = values_.cbegin();
    };

    void nextTuple() override{
        values_it_++;
    };

    bool is_end() const override{
        return values_it_ == values_.end();
    };

    Rid &rid() override {
        return _abstract_rid;
    };

    std::unique_ptr<RmRecord> Next() override {
        char* buf = new char[tupleLen_];
        memset(buf, 0, tupleLen_);
        Value value = *values_it_;
        value.init_raw(static_cast<int>(tupleLen_));
        memcpy(buf, value.raw->data, tupleLen_);
        return std::make_unique<RmRecord>(tupleLen_, value.raw->data);
    }
private:
    size_t tupleLen_;
    std::vector<Value> values_;
    std::vector<ColMeta> cols_;
    std::vector<Value>::const_iterator values_it_;
};
