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
        int num = wantAttrs.size();
        memcpy(wantRecord, &num, sizeof(int));
        wantLength += sizeof(int) + sizeof(short) * wantAttrs.size();
        int ptrPosition = sizeof(int);

        unsigned short length = 0;
        char *record = (char *)data2record(getData, getAttrs, length);
        for (unsigned i = 0; i < attrsIdx.size(); i++)
        {
            unsigned short startIdx;
            unsigned short endIdx;
            if (attrsIdx[i] == 0) {
                startIdx = sizeof(short) * getAttrs.size() + sizeof(int);
            }
            else {
                unsigned short offset;
                memcpy(&offset, record + sizeof(int) + 2 * (attrsIdx[i] - 1), sizeof(short));
                startIdx = sizeof(short) * getAttrs.size() + sizeof(int) + offset;
            }
            unsigned short offset;
            memcpy(&offset, record + sizeof(int) + 2 * attrsIdx[i], sizeof(short));
            endIdx = sizeof(short) * getAttrs.size() + sizeof(int) + offset;
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
    string attrName = condition.lhsAttr;
    leftAttrId = find_if(leftAttrs.begin(), leftAttrs.end(), [attrName] (const Attribute& attr) {
                            return attr.name == attrName;
                        }) - leftAttrs.begin();
    rightIn->getAttributes(rightAttrs);
    attrName = condition.rhsAttr;
    rightAttrId = find_if(rightAttrs.begin(), rightAttrs.end(), [attrName] (const Attribute& attr) {
                            return attr.name == attrName;
                        }) - rightAttrs.begin();
    getAttributes(wantAttr);
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

RC BNLJoin::loadPages(Iterator *input, queue<void *> &curData, queue<void *> &nextData, int maxLen, const vector<Attribute> &attrs) {
    queue<void *>().swap(curData); // clear curData
    if (!nextData.empty()) {
        curData.push(nextData.front());
        nextData.pop();
    }
    int curLen = 0;
    
    while(true){
        void* data = malloc(PAGE_SIZE); // TODO: free at appropiate time
        RC rc = input->getNextTuple(data);
        if (rc == QE_EOF) {
            break;
        }
        int len = dataLength(data, attrs);
        curLen += len;
        if (curLen > maxLen) {
            nextData.push(data);
            free(data);
            return 0;
        }
        curData.push(data);
    }
    return QE_EOF;
}

void BNLJoin::buildMap() {
    while(leftData.size() > 0){
        void* data = leftData.front();
        leftData.pop();
        unsigned short length = 0;
        char* record = (char *)data2record(data, leftAttrs, length);
        switch (leftAttrs[leftAttrId].type)
        {
            case TypeInt:
            {
                unsigned short startIdx;
                
                if (leftAttrId == 0) {
                    startIdx = sizeof(short) * leftAttrs.size() + sizeof(int);
                }
                else {
                    unsigned short offset;
                    memcpy(&offset, record + sizeof(int) + 2 * (leftAttrId - 1), sizeof(short));
                    startIdx = sizeof(short) * leftAttrs.size() + sizeof(int) + offset;
                }

                int val;
                memcpy(&val, record + startIdx, sizeof(int));
                intMap[val].push_back(data);
                break;
            }
                
            case TypeReal:
            {
                unsigned short startIdx;
                
                if (leftAttrId == 0) {
                    startIdx = sizeof(short) * leftAttrs.size() + sizeof(int);
                }
                else {
                    unsigned short offset;
                    memcpy(&offset, record + sizeof(int) + 2 * (leftAttrId - 1), sizeof(short));
                    startIdx = sizeof(short) * leftAttrs.size() + sizeof(int) + offset;
                }

                float val;
                memcpy(&val, record + startIdx, sizeof(float));
                floatMap[val].push_back(data);
                break;
            }
                
            case TypeVarChar:
            {
                unsigned short startIdx;
                
                if (leftAttrId == 0) {
                    startIdx = sizeof(short) * leftAttrs.size() + sizeof(int);
                }
                else {
                    unsigned short offset;
                    memcpy(&offset, record + sizeof(int) + 2 * (leftAttrId - 1), sizeof(short));
                    startIdx = sizeof(short) * leftAttrs.size() + sizeof(int) + offset;
                }

                int l;
                memcpy(&l, record + startIdx, sizeof(int));
                char* val = new char[PAGE_SIZE];
                memcpy(&val, record + startIdx + sizeof(int), l);
                string str(val, l);
                stringMap[str].push_back(data);

                delete[] val;
                break;
            }
                
            default:
                break;
        }
        delete[] record;
    }
}

RC BNLJoin::getNextTuple(void *data) {
    /*
    if (outputBuffer not empty) {
        pop outputBuffer
        return 0
    }

    while (load left pages != QE_EOF) {
        build map
        while (load right page != QE_EOF) {
            while (rightData not empty) {
                pop rightData
                check in map
                append to outputBuffer
            }
        }
        if (outputBuffer not empty) {
            pop outputBuffer
            return 0
        }
    }
    return QE_EOF
    */

    if (!outputBuffer.empty()) {
        void* front = outputBuffer.front();
        int len = dataLength(front, wantAttr);
        memcpy(data, front, len);
        outputBuffer.pop_front();
        free(front);
        return 0;
    }

    
    while(true){
        RC rc = loadPages(leftIn, leftData, leftNextData, numPages * PAGE_SIZE, leftAttrs);
        if (rc == QE_EOF && leftData.size() == 0) {
            break;
        }
        buildMap();
        while(true){
            rc = loadPages(rightIn, rightData, rightNextData, PAGE_SIZE, rightAttrs);
            if (rc == QE_EOF && rightData.size() == 0) {
                break;
            }
            while(!rightData.empty()){
                void* rightFront = rightData.front(); //TODO: free
                rightData.pop();
                unsigned short length = 0;
                char* record = (char *)data2record(rightFront, rightAttrs, length);
                deque<void *> matched;
                switch (rightAttrs[rightAttrId].type)
                {
                    case TypeInt:
                    {
                        unsigned short startIdx;
                        
                        if (rightAttrId == 0) {
                            startIdx = sizeof(short) * rightAttrs.size() + sizeof(int);
                        }
                        else {
                            unsigned short offset;
                            memcpy(&offset, record + sizeof(int) + 2 * (rightAttrId - 1), sizeof(short));
                            startIdx = sizeof(short) * rightAttrs.size() + sizeof(int) + offset;
                        }

                        int val;
                        memcpy(&val, record + startIdx, sizeof(int));
                        deque<void *>::iterator it = matched.end();
                        matched.insert(it, intMap[val].begin(), intMap[val].end());
                        break;
                    }
                        
                    case TypeReal:
                    {
                        unsigned short startIdx;
                        
                        if (rightAttrId == 0) {
                            startIdx = sizeof(short) * rightAttrs.size() + sizeof(int);
                        }
                        else {
                            unsigned short offset;
                            memcpy(&offset, record + sizeof(int) + 2 * (rightAttrId - 1), sizeof(short));
                            startIdx = sizeof(short) * rightAttrs.size() + sizeof(int) + offset;
                        }

                        float val;
                        memcpy(&val, record + startIdx, sizeof(float));
                        deque<void *>::iterator it = matched.end();
                        matched.insert(it, floatMap[val].begin(), floatMap[val].end());
                        break;
                    }
                        
                    case TypeVarChar:
                    {
                        unsigned short startIdx;
                        
                        if (rightAttrId == 0) {
                            startIdx = sizeof(short) * rightAttrs.size() + sizeof(int);
                        }
                        else {
                            unsigned short offset;
                            memcpy(&offset, record + sizeof(int) + 2 * (rightAttrId - 1), sizeof(short));
                            startIdx = sizeof(short) * rightAttrs.size() + sizeof(int) + offset;
                        }

                        int l;
                        memcpy(&l, record + startIdx, sizeof(int));
                        char* val = new char[PAGE_SIZE];
                        memcpy(&val, record + startIdx + sizeof(int), l);
                        string str(val, l);
                        deque<void *>::iterator it = matched.end();
                        matched.insert(it, stringMap[str].begin(), stringMap[str].end());

                        delete[] val;
                        break;
                    }
                        
                    default:
                        break;
                }
                deque<void *>::iterator leftIt = matched.begin();
                deque<void *>::iterator it = outputBuffer.end();
                
                while(leftIt != matched.end()){
                    void* leftFront = matched.front();
                    void* joined = malloc(PAGE_SIZE);//TODO: free when appropriate
                    vector<char> nullBits(ceil((leftAttrs.size()+rightAttrs.size())/8.0), 0);
                    int i = 0;
                    for(int j=0;j<leftAttrs.size();j++){
                        char nullByte = *((char*)leftFront+j/8);
                        bitset<8> curBit(nullByte);
                        nullBits[i/8] += curBit[j%8]<<(i%8);
                        i++;
                    }
                    for(int j=0;j<rightAttrs.size();j++){
                        char nullByte = *((char*)rightFront+j/8);
                        bitset<8> curBit(nullByte);
                        nullBits[i/8] += curBit[j%8]<<(i%8);
                        i++;
                    }
                    int offset = 0;
                    int leftOffset = 0;
                    for(int i=0;i<nullBits.size();i++){
                        memcpy((char*)joined+offset, &(nullBits[i]), 1);
                        offset++;
                    }
                    for(i=0;i<leftAttrs.size();i++){
                        if(leftAttrs[i].type == TypeVarChar){
                            int l = leftAttrs[i].length;
                            memcpy((char*)joined+offset, &l, sizeof(int));
                            offset += 4;
                        }
                        memcpy((char*)joined+offset, (char*)leftFront+(int)ceil(leftAttrs.size()/8.0)+leftOffset, leftAttrs[i].length);
                        offset += leftAttrs[i].length;
                        leftOffset += leftAttrs[i].length;
                    }
                    int rightOffset = 0;
                    for(i=0;i<rightAttrs.size();i++){
                        if(rightAttrs[i].type == TypeVarChar){
                            int l = rightAttrs[i].length;
                            memcpy((char*)joined+offset, &l, sizeof(int));
                            offset += 4;
                        }
                        memcpy((char*)joined+offset, (char*)rightFront+(int)ceil(rightAttrs.size()/8.0)+rightOffset, rightAttrs[i].length);
                        offset += rightAttrs[i].length;
                        rightOffset += rightAttrs[i].length;
                    }
                    memset((char*)rightFront, 0, PAGE_SIZE);
                    outputBuffer.insert(it, joined);
                    leftIt++;
                    it++;
                }
                
                delete[] record;
            }
        }
        if (!outputBuffer.empty()) {
            void* front = outputBuffer.front();
            int len = dataLength(front, wantAttr);
            memcpy(data, front, len);
            outputBuffer.pop_front();
            free(front);
            return 0;
        }
    }
    return QE_EOF;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    for(int i=0;i<leftAttrs.size();i++){
		Attribute attr = leftAttrs[i];
		attrs.push_back(attr);
	}
	for(int i=0;i<rightAttrs.size();i++){
		Attribute attr = rightAttrs[i];
		attrs.push_back(attr);
    }
}

Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
    this->input = input;
    this->aggAttr = aggAttr;
    string attrName = aggAttr.name;
    this->op = op;
    input->getAttributes(getAttrs);
    attrId = find_if(getAttrs.begin(), getAttrs.end(), [attrName] (const Attribute& attr) {
                        return attr.name == attrName;
                    }) - getAttrs.begin();
}

RC Aggregate::getNextTuple(void *data) {
    char* getData = new char[PAGE_SIZE];
    float min = numeric_limits<float>::infinity();
    float max = -min;
    float sum = 0;
    float count = 0;
    void* num = malloc(sizeof(int));
    bool eof = true;
    
    while(input->getNextTuple(getData) != QE_EOF){
        eof = false;
        unsigned short length = 0;
        char *record = (char *)data2record(getData, getAttrs, length);
        unsigned short startIdx;
        if (attrId == 0) {
            startIdx = sizeof(short) * getAttrs.size() + sizeof(int);
        }
        else {
            unsigned short offset;
            memcpy(&offset, record + sizeof(int) + 2 * (attrId - 1), sizeof(short));
            startIdx = sizeof(short) * getAttrs.size() + sizeof(int) + offset;
        }
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
    if (eof) {
        delete[] getData;
        free(num);
        return QE_EOF;
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
    char nullIndicator = 1<<7;
    memcpy(data, &nullIndicator, sizeof(char));
    memcpy((char *)data + sizeof(char), &res, sizeof(float));
    
    delete[] getData;
    free(num);
    return 0;
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
	void* leftData = malloc(PAGE_SIZE);
	memset((char*)leftData, 0, PAGE_SIZE);
	void* rightData = malloc(PAGE_SIZE);
	memset((char*)rightData, 0, PAGE_SIZE);
	//rightIn->setIterator(NULL, NULL, false, false);
	while(leftIn->getNextTuple(leftData) != QE_EOF){
		unsigned short length = 0;
		//void* leftRecord = data2record(leftData, leftAttrs, length);
		string targetAttrName;
		targetAttrName = condition.lhsAttr;
		//targetAttrName = targetAttrName.substr(targetAttrName.find(".")+1);
		//cout<<targetAttrName<<endl;
		void* targetData = malloc(PAGE_SIZE);
		memset((char*)targetData,0,PAGE_SIZE);
		//readAttributeFromRecord(leftRecord, length, leftAttrs, targetAttrName, targetData);

		//get target data
		vector<bitset<8>> nullBytes;
		int offset = ceil(leftAttrs.size()/8.0);
		for(int i=0;i<offset;i++){
			nullBytes.push_back(bitset<8>((char*)leftData+i));
		}
		for(int i=0;i<leftAttrs.size();i++){
			if(nullBytes[i/8][i%8] == 1) continue;
			if(leftAttrs[i].name == targetAttrName){
				if(leftAttrs[i].type == 2){
					int l = 0;
					memcpy(&l, (char*)leftData+offset, 4);
					memcpy(targetData, (char*)leftData+offset, 4+l);
				} else{
					memcpy(targetData, (char*)leftData+offset, 4);
				}
				break;
			} else{
				if(leftAttrs[i].type == 2){
					int l = 0;
					memcpy(&l, (char*)leftData+offset, 4);
					offset = offset+4+l;
				} else{
					offset += 4;
				}
			}
		}
		//cout<<"targetData: "<<*(float*)((char*)targetData)<<endl;
		rightIn->setIterator(targetData, targetData, true, true);
		while(rightIn->getNextTuple(rightData) != QE_EOF){
			//cout<<"right"<<endl;
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
			memset((char*)rightData, 0, PAGE_SIZE);
			return SUCCESS;
		}
		free(targetData);
	}
	free(leftData);
	free(rightData);
	return QE_EOF;
}
