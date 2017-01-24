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

        public void OpenScan()
        { }

        // Passes back the key scanned and number of scanned items so
        // far (whether the predicate matched or not.
        Tuple<RID, TK,int> GetNextEntry()
        {
            int numScanned = -1;
            RIDKey<TK> value = new RIDKey<TK>();
            bool currDeleted = false;

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
                                if (EarlyExitOptimize(key))
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
                                if (EarlyExitOptimize(key))
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

        private bool EarlyExitOptimize(TK key)
        {
            if (!bOpen) throw new Exception();

            if (value == null)
                return true; //nothing to optimize
            // no opt possible
            if (c == CompOp.NE_OP || c == CompOp.NO_OP)
                return true;

            if (currNode != null)
            {
                int cmp = key.CompareTo(value);
                if (c == CompOp.EQ_OP && cmp != 0)
                {
                    eof = true;
                    return true;
                }
                if ((c == CompOp.LT_OP || c == CompOp.LE_OP) && cmp > 0 && !desc)
                {
                    eof = true;
                    return true;
                }
                if ((c == CompOp.GT_OP || c == CompOp.GE_OP) && cmp < 0 && desc)
                {
                    eof = true;
                    return true;
                }
            }
            return true;
        }

        private void OpOptimize()
        { }
    }
}
