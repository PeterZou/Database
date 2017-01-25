using Database;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

namespace Database.IndexManage
{
    public class IX_IndexScan<TK>
        where TK : IComparable<TK>
    {
        Predicate pred;
        IX_FileHandle<TK> pifh;
        int currPos;// in one node
        TK currKey; // saved to check for delete on scan
        RID currRid; // saved to check for delete on scan
        bool bOpen;
        bool desc; // Is scan order ascending(def) or descending ?
        bool eof; // early EOF set by btree analysis - set by OpOpt
        bool foundOne; // flag that is set by getNext if it is ever successful
        CompOp c; // save Op for OpOpt
        Node<TK, RIDKey<TK>> currNode;
        Node<TK, RIDKey<TK>> lastNode;
        TK value;

        public IX_IndexScan()
        {
            currPos = -1;
            currRid = new RID(-1, -1);
            c = CompOp.EQ_OP;
            bOpen = false;
            desc = false;
            eof = false;
        }

        public bool IsOpen() { return (bOpen && pred != null && pifh != null); }

        public void OpenScan(
                IX_FileHandle<TK> fileHandle,
                CompOp compOp,
                TK value_,
                ClientHint pinHint,
                bool desc)
        {
            if (bOpen) throw new Exception();

            if ((compOp < CompOp.NO_OP) ||
                compOp > CompOp.GE_OP)
                throw new Exception();


            pifh = fileHandle;
            if (pifh == null) throw new Exception();

            bOpen = true;
            if (desc)
                this.desc = true;
            foundOne = false;

            AttrType attrType = SetAttrType();

            pred = new Predicate(attrType,
                                 pifh.OccupiedNum(default(TK)),
                                 0,
                                 compOp,
                                 pifh.ConverTKToString(value_),
                                 pinHint);


            c = compOp;
            if (value_ != null)
            {
                value = value_; // TODO deep copy ?
                OpOptimize();
            }
        }

        private static AttrType SetAttrType()
        {
            var attrType = (AttrType)0;
            if (default(TK) is int)
            {
                attrType = AttrType.INT;
            }
            else if (default(TK) is float)
            {
                attrType = AttrType.FLOAT;
            }
            else
            {
                attrType = AttrType.STRING;
            }

            return attrType;
        }

        // Passes back the key scanned and number of scanned items so
        // far (whether the predicate matched or not.
        public Tuple<RID, TK,int> GetNextEntry()
        {
            int numScanned = -1;
            RIDKey<TK> value = new RIDKey<TK>();
            bool currDeleted = false;

            if (eof) return null;

            currDeleted = SetCurrNode(currDeleted);

            for (; (currNode != null);)
            {
                int index = -1;
                TK key = default(TK);
                if (!desc)
                {
                    Tuple<RID, TK, int> tuple = null;
                    // first time in for loop ?
                    if (!currDeleted)
                    {
                        index = currPos + 1;
                    }
                    else
                    {
                        index = currPos;
                        currDeleted = false;
                    }
                    for (; index < currNode.Values.Count; index++)
                    {
                        key = currNode.Values[index];
                        numScanned++;

                        currPos = index;
                        if (currKey == null)
                            currKey = default(TK);
                        currKey = key;
                        currRid = currNode.CurrentRID.Rid;

                        if (pred.Eval(pifh.ConverTKToString(key).ToArray(), pred.compOp))
                        {
                            foundOne = true;
                            tuple = new Tuple<RID, TK, int>(currNode.CurrentRID.Rid, key, numScanned);
                        }
                        else
                        {
                            if (foundOne)
                            {
                                EarlyExitOptimize(key);
                                if (eof)
                                    return null;
                            }
                        }
                    }
                    return tuple;

                }
                else
                {
                    // first time in for loop ?
                    if (!currDeleted)
                    {
                        index = currPos - 1;
                    }
                    else
                    {
                        index = currPos;
                        currDeleted = false;
                    }
                    for (; index >= 0; index--)
                    {
                        key = currNode.Values[index];
                        numScanned++;

                        currPos = index;
                        if (currKey == null)
                            currKey = default(TK);
                        currKey = key;
                        currRid = currNode.CurrentRID.Rid;

                        if (pred.Eval(pifh.ConverTKToString(key).ToArray(), pred.compOp))
                        {
                            foundOne = true;
                            return new Tuple<RID, TK, int>(currNode.CurrentRID.Rid, key, numScanned);
                        }
                        else
                        {
                            if (foundOne)
                            {
                                EarlyExitOptimize(key);
                                if (eof)
                                    return null;
                            }
                        }
                    }
                }
                if ((lastNode != null) && currNode.CurrentRID.Rid.CompareTo(lastNode.CurrentRID.Rid) == 0)
                    break;

                // Advance to a new page
                if (!desc)
                {
                    currNode = pifh.FindNextLeafNode(currNode);
                    currPos = -1;
                }
                else
                {
                    currNode = pifh.FindPreviousLeafNode(currNode);
                    currPos = currNode.Values.Count;
                }
            }

            return null;
        }

        public void CloseScan()
        {
            if (!bOpen) throw new Exception();
            bOpen = false;
            pred = null;
            currNode = null;
            currPos = -1;
            if (currKey.CompareTo(default(TK)) != 0)
            {
                currKey = default(TK);
            }
            currRid = new RID(-1, -1);
            lastNode = null;
            eof = false;
        }

        public void ResetState()
        {
            currNode = null;
            currPos = -1;
            lastNode = null;
            eof = false;
            foundOne = false;

            OpOptimize();
        }

        private bool SetCurrNode(bool currDeleted)
        {
            if (!bOpen) throw new Exception();

            // first time in
            if (currNode == null && currPos == -1)
            {
                if (!desc)
                {
                    currNode = pifh.FindSmallestLeaf();
                    currPos = -1;
                }
                else
                {
                    currNode = pifh.FindLargestLeaf();
                    currPos = currNode.Property.Count; // 1 past
                }
                currDeleted = false;
            }
            else
            { // check if the entry at curr was deleted
                if (currKey != null && currNode != null && currPos != -1)
                {
                    TK key = currNode.Values[currPos];

                    if (currKey.CompareTo(key) != 0)
                    {
                        currDeleted = true;
                    }
                    else
                    {
                        if (currRid.Page != currNode.CurrentRID.Rid.Page || currRid.Page != currNode.CurrentRID.Rid.Page)
                            currDeleted = true;
                    }
                }
            }

            return currDeleted;
        }

        private void EarlyExitOptimize(TK key)
        {
            if (!bOpen) throw new Exception();

            if (value == null)
                return; //nothing to optimize
            // no opt possible
            if (c == CompOp.NE_OP || c == CompOp.NO_OP)
                return;
            if (currNode != null)
            {
                int cmp = key.CompareTo(value);
                if (c == CompOp.EQ_OP && cmp != 0)
                {
                    eof = true;
                }
                else if ((c == CompOp.LT_OP || c == CompOp.LE_OP) && cmp > 0 && !desc)
                {
                    eof = true;
                }
                else if ((c == CompOp.GT_OP || c == CompOp.GE_OP) && cmp < 0 && desc)
                {
                    eof = true;
                }
                return;
            }
            return;
        }

        private void OpOptimize()
        {
            if (!bOpen) throw new Exception();

            if (value == null)
                return; //nothing to optimize

            // no opt possible
            if (c == CompOp.NE_OP)
                return;

            // hack for indexscan::OpOptimize
            // FindLeaf() does not really return rightmost node that has a key. This happens
            // when there are duplicates that span multiple btree nodes.
            // The strict rightmost guarantee is mainly required for
            // What if have a duplicated value ,so turn right to a new value
            currNode = pifh.FindNextLeafForceNode(currNode,value);
            currPos = currNode.Values.IndexOf(value);

            if (desc == true)
            {
                // find rightmost version of a value and go left from there.
                if (c == CompOp.LE_OP || c == CompOp.LT_OP)
                {
                    lastNode = null;
                    currPos = currPos + 1; // go one past
                }

                if (c == CompOp.EQ_OP)
                {
                    if (currPos == -1)
                    {// key does not exist
                        eof = true;
                        return;
                    }
                    // reset cause you could miss first value
                    lastNode = null;
                    currPos = currPos + 1; // go one past
                }

                // find rightmost version of value lesser than and go left from there.
                if (c == CompOp.GE_OP)
                {
                    lastNode = null;
                    currNode = null;
                    currPos = -1;
                }

                if (c == CompOp.GT_OP)
                {
                    lastNode = pifh.ImportOneLeafNode(currNode.CurrentRID.Rid);
                    currNode = null;
                    currPos = -1;
                }
            }
            else
            {
                if ((c == CompOp.LE_OP || c == CompOp.LT_OP))
                {
                    lastNode = pifh.ImportOneLeafNode(currNode.CurrentRID.Rid);
                    currNode = null;
                    currPos = -1;
                }
                if ((c == CompOp.GT_OP))
                {
                    lastNode = null;
                    // currNode = pixh->FetchNode(currNode->GetPageRID());
                    // currNode->Print(cerr);
                    // cerr << "GT curr was " << currNode->GetPageRID() << endl;
                }
                if ((c == CompOp.GE_OP))
                {
                    currNode = null;
                    currPos = -1;
                    lastNode = null;
                }
                if ((c == CompOp.EQ_OP))
                {
                    if (currPos == -1)
                    { // key does not exist
                        eof = true;
                        return;
                    }
                    lastNode = pifh.ImportOneLeafNode(currNode.CurrentRID.Rid);
                    currNode = null;
                    currPos = -1;
                }
            }
            return;
        }
    }
}
