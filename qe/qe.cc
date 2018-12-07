
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    this->input = input;
    this->condition = condition;
    input->getAttributes(attrs);
    lhsValue = malloc(PAGE_SIZE); //TODO: free at ~Filter()
    rhsValue = malloc(PAGE_SIZE); //TODO: free at ~Filter()
}

// ... the rest of your implementations go here

RC Filter::getNextTuple(void *data) {
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

        while(input->getNextTuple(data) != QE_EOF){
            int offset = 0;
            int nullIndSize = ceil((double)attrs.size()/(double)8);
            offset += nullIndSize;
            for(unsigned i = 0; i < attrs.size(); i++)
            {
                if (attrs.at(i).name == condition.lhsAttr) {
                    switch (attrs[i].type)
                    {
                        case TypeVarChar:
                            int length;
                            memcpy(&length, (char *)data + offset, sizeof(int));
                            memcpy(lhsValue, (char *)data + offset, sizeof(int) + length);
                            offset += sizeof(int) + length;
                            break;
                    
                        default:
                            memcpy(lhsValue, (char *)data + offset, sizeof(int));
                            offset += sizeof(int);
                            break;
                    }
                    if (compare(condition.op, attrs[i].type)) {
                        return 0;
                    }
                    break;
                }
                else {
                    if (attrs[i].type == TypeVarChar) {
                        int length;
                        memcpy(&length, (char *)data + offset, sizeof(int));
                        offset += sizeof(int) + length;
                    }
                    else {
                        offset += sizeof(int);
                    }
                }
            }
        }
    }
    else {
        while(input->getNextTuple(data) != QE_EOF) {
            int offset = 0;
            int nullIndSize = ceil((double)attrs.size()/(double)8);
            offset += nullIndSize;
            bool findLeft = false;
            bool findRight = false;
            
            for(unsigned i = 0; i < attrs.size(); i++)
            {
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
                        if (compare(condition.op, attrs[i].type)) {
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
    }
}
