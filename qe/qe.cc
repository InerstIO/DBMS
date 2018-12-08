
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    this->input = input;
    this->condition = condition;
    input->getAttributes(attrs);
}

// ... the rest of your implementations go here

RC Filter::getNextTuple(void *data) {
    char* lhsValue = new char[PAGE_SIZE];
    char* rhsValue = new char[PAGE_SIZE];

    if (!condition.bRhsIsAttr) {
        // get rhsValue
        switch (condition.rhsValue.type)
        {
            case TypeVarChar:
                int length;
                memcpy(&length, condition.rhsValue.data, sizeof(int));
                memcpy(rhsValue, condition.rhsValue.data, sizeof(int) + length);
                break;
        
            default:
                memcpy(rhsValue, condition.rhsValue.data, sizeof(int));
                break;
        }
    }

    while(input->getNextTuple(data) != QE_EOF) {
        int offset = 0;
        int nullIndSize = ceil((double)attrs.size()/(double)8);
        vector<bitset<8>> nullBits = nullIndicators(nullIndSize, data);
        offset += nullIndSize;
        bool findLeft = false;
        bool findRight = !condition.bRhsIsAttr;
        
        for(unsigned i = 0; i < attrs.size(); i++)
        {
            // if null attribute
            if(!nullBits[i/8][7-i%8]) {
                continue;
            }
            if (attrs.at(i).name == condition.lhsAttr || attrs.at(i).name == condition.rhsAttr) {
                
                switch (attrs[i].type)
                {
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char *)data + offset, sizeof(int));
                        if (attrs.at(i).name == condition.lhsAttr) {
                            memcpy(lhsValue, (char *)data + offset, sizeof(int) + length);
                            findLeft = true;
                        }
                        else {
                            memcpy(rhsValue, (char *)data + offset, sizeof(int) + length);
                            findRight = true;
                        }
                        offset += sizeof(int) + length;
                        break;
                
                    default:
                        if (attrs.at(i).name == condition.lhsAttr) {
                            memcpy(lhsValue, (char *)data + offset, sizeof(int));
                            findLeft = true;
                        }
                        else {
                            memcpy(rhsValue, (char *)data + offset, sizeof(int));
                            findRight = true;
                        }
                        offset += sizeof(int);
                        break;
                }

                if (findLeft && findRight) {
                    if (compare(condition.op, attrs[i].type, lhsValue, rhsValue)) {
                        delete[] lhsValue;
                        delete[] rhsValue;
                        return 0;
                    }
                    break;
                }
            }
            else {
                switch (attrs[i].type)
                {
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char *)data + offset, sizeof(int));
                        offset += sizeof(int) + length;
                        break;
                
                    default:
                        offset += sizeof(int);
                        break;
                }
            }
        }
    }
    delete[] lhsValue;
    delete[] rhsValue;
    return QE_EOF;
}

bool Filter::compare(CompOp op, AttrType type, char* lhsValue, char* rhsValue) {
    bool result = false;
    switch (type)
    {
        case TypeInt:
        {
            int leftVal;
            int rightVal;
            memcpy(&leftVal, lhsValue, sizeof(int));
            memcpy(&rightVal, rhsValue, sizeof(int));
            switch (op)
            {
                case EQ_OP:
                    result = leftVal == rightVal;
                    break;
                case LT_OP:
                    result = leftVal < rightVal;
                    break;
                case LE_OP:
                    result = leftVal <= rightVal;
                    break;
                case GT_OP:
                    result = leftVal > rightVal;
                    break;
                case GE_OP:
                    result = leftVal >= rightVal;
                    break;
                case NE_OP:
                    result = leftVal != rightVal;
                    break;
                case NO_OP:
                    break;
                default:
                    break;
            }
            break;
        }
        case TypeReal:
        {
            float leftVal;
            float rightVal;
            memcpy(&leftVal, lhsValue, sizeof(float));
            memcpy(&rightVal, rhsValue, sizeof(float));
            switch (op)
            {
                case EQ_OP:
                    result = leftVal == rightVal;
                    break;
                case LT_OP:
                    result = leftVal < rightVal;
                    break;
                case LE_OP:
                    result = leftVal <= rightVal;
                    break;
                case GT_OP:
                    result = leftVal > rightVal;
                    break;
                case GE_OP:
                    result = leftVal >= rightVal;
                    break;
                case NE_OP:
                    result = leftVal != rightVal;
                    break;
                case NO_OP:
                    break;
                default:
                    break;
            }
            break;
        }
        case TypeVarChar:
        {
            int leftLength;
            int rightLength;
            memcpy(&leftLength, lhsValue, sizeof(int));
            memcpy(&rightLength, rhsValue, sizeof(int));
            char* leftVal = new char[leftLength];
            char* rightVal = new char[rightLength];
            memcpy(leftVal, (char *)lhsValue + sizeof(int), leftLength);
            memcpy(rightVal, (char *)rhsValue + sizeof(int), rightLength);
            switch (op)
            {
                case EQ_OP:
                    result = strcmp(leftVal, rightVal) == 0;
                    break;
                case LT_OP:
                    result = strcmp(leftVal, rightVal) < 0;
                    break;
                case LE_OP:
                    result = strcmp(leftVal, rightVal) <= 0;
                    break;
                case GT_OP:
                    result = strcmp(leftVal, rightVal) > 0;
                    break;
                case GE_OP:
                    result = strcmp(leftVal, rightVal) >= 0;
                    break;
                case NE_OP:
                    result = strcmp(leftVal, rightVal) != 0;
                    break;
                case NO_OP:
                    break;
                default:
                    break;
            }
            delete[] leftVal;
            delete[] rightVal;
            break;
        }
        default:
            break;
    }
    
    return result;
}

void Filter::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    input->getAttributes(attrs);
}

Project::Project(Iterator *input, const vector<string> &attrNames) {
    this->input = input;
    this->attrNames = attrNames;
    input->getAttributes(getAttrs);
    getData = malloc(PAGE_SIZE);
    attrNotFound = false;

    map<string, int> attrsMap;
    
    for(unsigned i = 0; i < getAttrs.size(); i++)
    {
        attrsMap.insert(pair<string, int>(getAttrs[i].name, i));
    }
    
    map<string, int>::iterator it;
    
    for(unsigned i = 0; i < attrNames.size(); i++)
    {
        it = attrsMap.find(attrNames[i]);
        if (it == attrsMap.end()) {
            attrNotFound = true;
        }
        attrsIdx.push_back(it->second);
    }
    // getAttributes after attrsIdx is initialized
    getAttributes(wantAttrs);
}

Project::~Project() {
    free(getData);
}

RC Project::getNextTuple(void *data) {
    if (attrNotFound) {
        return -1;
    }

    if (input->getNextTuple(getData) != QE_EOF) {
        void* wantRecord = malloc(PAGE_SIZE);
        int wantLength = 0;
        short num = wantAttrs.size();
        memcpy(wantRecord, &num, sizeof(short));
        wantLength += sizeof(short) + 2 * wantAttrs.size();
        int ptrPosition = sizeof(short);

        unsigned short length = 0;
        char *record = (char *)data2record(getData, getAttrs, length);
        for (unsigned i = 0; i < attrsIdx.size(); i++)
        {
            unsigned short startIdx;
            unsigned short endIdx;
            memcpy(&startIdx, record + sizeof(int) + 2 * attrsIdx[i], sizeof(short));
            if (attrsIdx[i] == getAttrs.size() - 1) {
                endIdx = length;
            }
            else {
                memcpy(&endIdx, record + sizeof(int) + 2 * (attrsIdx[i] + 1), sizeof(short));
            }
            memcpy((char *)wantRecord + wantLength, record + startIdx, endIdx - startIdx);
            memcpy((char *)wantRecord + ptrPosition, &wantLength, sizeof(short));
            wantLength += endIdx - startIdx;
            ptrPosition += sizeof(short);
        }
        record2data(wantRecord, wantAttrs, data);
        free(wantRecord);
        return 0;
    }
    
    return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    for(unsigned i = 0; i < attrsIdx.size(); i++)
    {
        attrs.push_back(getAttrs[attrsIdx[i]]);
    }
}
