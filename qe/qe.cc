#include <algorithm>
#include <limits>
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
            if(nullBits[i/8][7-i%8]) {
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
            char* leftVal = new char[leftLength + 1];
            char* rightVal = new char[rightLength + 1];
            memcpy(leftVal, (char *)lhsValue + sizeof(int), leftLength);
            memcpy(rightVal, (char *)rhsValue + sizeof(int), rightLength);
            char end = '\0';
            memcpy(leftVal + leftLength, &end, sizeof(char));
            memcpy(rightVal + rightLength, &end, sizeof(char));
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
        char* wantRecord = new char[PAGE_SIZE];
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
        delete[] wantRecord;
        delete[] record;
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

BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
    this->leftIn = leftIn;
    this->rightIn = rightIn;
    this->condition = condition;
    this->numPages = numPages;
    leftIn->getAttributes(leftAttrs);
    //leftAttrId = find(leftAttrs.begin(), leftAttrs.end(), condition.lhsAttr) - leftAttrs.begin();
}

int BNLJoin::dataLength(const void* data, const vector<Attribute>& recordDescriptor) {
    int dataPos = 0;
    int attrNum = recordDescriptor.size();
    int nullIndSize = ceil((double)attrNum/(double)8);
    vector<bitset<8>> nullBits = nullIndicators(nullIndSize, data);
    dataPos += nullIndSize;
    for(int i=0;i<attrNum;i++){
        if(nullBits[i/8][7-i%8] == 0){
            if(recordDescriptor[i].type == 0 || recordDescriptor[i].type == 1){
                dataPos += 4;
            } else{
                int stringLength;
                memcpy(&stringLength, (char*)data+dataPos, sizeof(int));
                dataPos += 4 + stringLength;
            }
        }
    }
    return dataPos;
}

RC BNLJoin::loadPages(Iterator *input, DataDir dataDir, DataDir nextData, int maxLen, const vector<Attribute> &attrs) {
    if (nextData.data.size() > 0) {
        dataDir.data.push(nextData.data.front());
        dataDir.length.push(nextData.length.front());
        nextData.data.pop();
        nextData.length.pop();
    }
    int curLen = 0;
    
    void* data = malloc(PAGE_SIZE);
    while(input->getNextTuple(data) != QE_EOF){
        int len = dataLength(data, attrs);
        curLen += len;
        if (curLen > maxLen) {
            nextData.data.push(data);
            nextData.length.push(len);
            free(data);
            return 0;
        }
        dataDir.data.push(data);
        dataDir.length.push(len);
    }
    free(data);
    return QE_EOF;
}

void BNLJoin::buildMap() {
    while(leftData.data.size() > 0){
        void* data = leftData.data.front();
        leftData.data.pop();
        int len = leftData.length.front();
        leftData.length.pop();
        unsigned short length = 0;
        void* record = data2record(data, leftAttrs, length);
        switch (leftAttrs[leftAttrId].type)
        {
            case TypeInt:
            {
                short offset;
                memcpy(&offset, (char *)record + sizeof(int) + sizeof(short) * leftAttrId, sizeof(short));
                int val;
                memcpy(&val, (char *)record + offset, sizeof(int));
                if (intMap.find(val) == intMap.end()) {
                    DataDir dataDir;
                    dataDir.data.push(data);
                    dataDir.length.push(len);
                    intMap[val] = dataDir;
                }
                else {
                    intMap[val].data.push(data);
                    intMap[val].length.push(len);
                }
                break;
            }
                
            case TypeReal:
            {
                short offset;
                memcpy(&offset, (char *)record + sizeof(int) + sizeof(short) * leftAttrId, sizeof(short));
                float val;
                memcpy(&val, (char *)record + offset, sizeof(float));
                if (floatMap.find(val) == floatMap.end()) {
                    DataDir dataDir;
                    dataDir.data.push(data);
                    dataDir.length.push(len);
                    floatMap[val] = dataDir;
                }
                else {
                    floatMap[val].data.push(data);
                    floatMap[val].length.push(len);
                }
                break;
            }
                
            case TypeVarChar:
            {
                short offset;
                memcpy(&offset, (char *)record + sizeof(int) + sizeof(short) * leftAttrId, sizeof(short));
                int l;
                memcpy(&l, (char *)record + offset, sizeof(int));
                char* val = new char[PAGE_SIZE]; //TODO: delete[]
                memcpy(&val, (char *)record + offset + sizeof(int), l);
                string str(val, l);

                if (stringMap.find(str) == stringMap.end()) {
                    DataDir dataDir;
                    dataDir.data.push(data);
                    dataDir.length.push(len);
                    stringMap[str] = dataDir;
                }
                else {
                    stringMap[str].data.push(data);
                    stringMap[str].length.push(len);
                }
                break;
            }
                
            default:
                break;
        }
    }
}

/*RC BNLJoin::getNextTuple(void *data) {
    
}*/

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
    this->input = input;
    this->aggAttr = aggAttr;
    this->op = op;
    input->getAttributes(getAttrs);
    //attrId = find(getAttrs.begin(), getAttrs.end(), aggAttr) - getAttrs.begin();
}

RC Aggregate::getNextTuple(void *data) {
    char* getData = new char[PAGE_SIZE];
    float min = numeric_limits<float>::infinity();
    float max = -min;
    float sum = 0;
    float count = 0;
    void* num = malloc(sizeof(int));
    
    while(input->getNextTuple(getData) != QE_EOF){
        unsigned short length = 0;
        char *record = (char *)data2record(getData, getAttrs, length);
        unsigned short startIdx;
        memcpy(&startIdx, record + sizeof(int) + 2 * attrId, sizeof(short));
        memcpy(num, record + startIdx, sizeof(int));
        delete[] record;
        float val;
        if (aggAttr.type == TypeInt) {
            val = (float)*(int *)num;
        }
        else {
            val = *(float *)num;
        }
        
        switch (op)
        {
            case MAX:
                if (val > max) {
                    max = val;
                }
                break;
            case MIN:
                if (val < min) {
                    min = val;
                }
                break;
            case COUNT:
                count++;
                break;
            case SUM:
                sum += val;
                break;
            case AVG:
                count++;
                sum += val;
                break;
            default:
                break;
        }
    }
    float res;
    switch (op)
    {
        case MAX:
            res = max;
            break;
        case MIN:
            res = min;
            break;
        case COUNT:
            res = count;
            break;
        case SUM:
            res = sum;
            break;
        case AVG:
            res = sum / count;
            break;
        default:
            break;
    }

    memcpy(data, &res, sizeof(float));
    
    delete[] getData;
    free(num);
    return QE_EOF;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
    attrs.clear();
    Attribute attr;
    attr.type = TypeReal;
    attr.length = sizeof(float);
    string aggOp;
    
    switch (op)
    {
        case MAX:
            aggOp = "MAX";
            break;
        case MIN:
            aggOp = "MIN";
            break;
        case COUNT:
            aggOp = "COUNT";
            break;
        case SUM:
            aggOp = "SUM";
            break;
        case AVG:
            aggOp = "AVG";
            break;
        default:
            break;
    }
    attr.name = aggOp + "(" + aggAttr.name + ")";
    attrs.push_back(attr);
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition){
	this->leftIn = leftIn;
	this->rightIn = rightIn;
	this->condition = condition;
	leftIn->getAttributes(leftAttrs);
	rightIn->getAttributes(rightAttrs);
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	for(int i=0;i<leftAttrs.size();i++){
		Attribute attr = leftAttrs[i];
		attrs.push_back(attr);
	}
	for(int i=0;i<rightAttrs.size();i++){
		Attribute attr = rightAttrs[i];
		attrs.push_back(attr);
	}
}

RC INLJoin::getNextTuple(void *data){
	void* leftData = malloc(4000);
	memset((char*)leftData, 0, 4000);
	void* rightData = malloc(4000);
	memset((char*)rightData, 0, 4000);
	while(leftIn->getNextTuple(leftData) != QE_EOF){
		unsigned short length = 0;
		void* leftRecord = data2record(leftData, leftAttrs, length);
		string targetAttrName;
		targetAttrName = condition.lhsAttr;
		void* targetData = malloc(4000);
		memset((char*)targetData,0,4000);
		readAttributeFromRecord(leftRecord, length, leftAttrs, targetAttrName, targetData);
		cout<<"targetData: "<<*(int*)((char*)targetData+1)<<endl;
		rightIn->setIterator(targetData, targetData, true, true);
		while(rightIn->getNextTuple(rightData) != QE_EOF){
			vector<char> nullBits(ceil((leftAttrs.size()+rightAttrs.size())/8.0), 0);
			int i = 0;
			for(int j=0;j<leftAttrs.size();j++){
				char nullByte = *((char*)leftData+j/8);
				bitset<8> curBit(nullByte);
				nullBits[i/8] += curBit[j%8]<<(i%8);
				i++;
			}
			for(int j=0;j<rightAttrs.size();j++){
				char nullByte = *((char*)rightData+j/8);
				bitset<8> curBit(nullByte);
				nullBits[i/8] += curBit[j%8]<<(i%8);
				i++;
			}
			int offset = 0;
			int leftOffset = 0;
			for(int i=0;i<nullBits.size();i++){
				memcpy((char*)data+offset, &(nullBits[i]), 1);
				offset++;
			}
			for(i=0;i<leftAttrs.size();i++){
				if(leftAttrs[i].type == TypeVarChar){
					int l = leftAttrs[i].length;
					memcpy((char*)data+offset, &l, sizeof(int));
					offset += 4;
				}
				memcpy((char*)data+offset, (char*)leftData+(int)ceil(leftAttrs.size()/8.0)+leftOffset, leftAttrs[i].length);
				offset += leftAttrs[i].length;
				leftOffset += leftAttrs[i].length;
			}
			int rightOffset = 0;
			for(i=0;i<rightAttrs.size();i++){
				if(rightAttrs[i].type == TypeVarChar){
					int l = rightAttrs[i].length;
					memcpy((char*)data+offset, &l, sizeof(int));
					offset += 4;
				}
				memcpy((char*)data+offset, (char*)rightData+(int)ceil(rightAttrs.size()/8.0)+rightOffset, rightAttrs[i].length);
				offset += rightAttrs[i].length;
				rightOffset += rightAttrs[i].length;
			}
			memset((char*)rightData, 0, 4000);
		}
		free(targetData);
	}
	free(leftData);
	free(rightData);
	return QE_EOF;
}

RC readAttributeFromRecord(const void* record, unsigned short length, const vector<Attribute> &recordDescriptor, const string &attributeName, void *data) {
    unsigned i;
    short base = sizeof(short)*recordDescriptor.size()+4-1;
    short startAddr = sizeof(short)*recordDescriptor.size()+4;
    short endAddr = startAddr-1;
    bool isNull = false;
    for(i = 0; i < recordDescriptor.size(); i++)
    {
        short pointer;
        memcpy(&pointer, (char*)record+i*sizeof(short)+4, sizeof(short));
                //cout<<pointer<<", ";
        if (recordDescriptor[i].name == attributeName) {
        	cout<<recordDescriptor[i].name<<", "<<attributeName<<endl;
            if(pointer == -1){
                isNull = true;
            } else{
                startAddr = endAddr+1;
                endAddr = pointer+base;
               // cout<<startAddr<<"--"<<base<<"+"<<pointer<<endl;
            }
            break;
        } else{
            if(pointer != -1){
                startAddr = endAddr+1;
                endAddr = pointer+base;
            }
        }
    }
    //cout<<endl;
    if (i == recordDescriptor.size()) {
        return -1;
    }
    
    char nullIndicator;
    if(isNull){
        nullIndicator = 255;
    } else{
        nullIndicator = 0;
    }
    ((char*)data)[0] = nullIndicator;
    if(recordDescriptor[i].type == 2){
        length = endAddr-startAddr+1;
        //cout<<length<<endl;
        memcpy((char*)data+1, &length, sizeof(int));
        memcpy((char*)data+5, (char*)record+startAddr, length);
    } else{
        length = 4;
        memcpy((char*)data+1, (char*)record+startAddr, 4);
        cout<<"startAddr: "<<startAddr<<endl;
        cout<<*(int*)((char*)record+startAddr)<<endl;
    }
    return 0;
}