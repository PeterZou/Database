using Database.IndexManage;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using Database.SQLOperation;
using Database.SQLOperation.data;
using Database.SystemManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.QueryManage
{
    public class QL_Manager<TK> where TK : IComparable<TK>
    {
        RM_Manager rmm;
        IX_Manager<TK> ixm;
        SM_Manager<TK> smm;
        Func<TK, string> ConverTKToString { get; set; }
        Func<string, TK> ConvertStringToTK { get; set; }
        Func<TK, int> OccupiedNum { get; set; }

        //
        // Constructor for the QL Manager
        //
        public QL_Manager(SM_Manager<TK> smm_, IX_Manager<TK> ixm_, RM_Manager rmm_,
            Func<TK, string> converTKToString,
            Func<TK, int> occupiedNum,
            Func<string, TK> convertStringToTK)
        {
            this.ConvertStringToTK = ConvertStringToTK;
            this.ConverTKToString = converTKToString;
            this.OccupiedNum = occupiedNum;
            this.smm = smm_;
            this.ixm = ixm_;
            this.rmm = rmm_;
        }

        public void PrintIterator(OperationIterator it)
        {
            throw new NotImplementedException();
        }

        public void Select(int nSelAttrs, AggRelAttr[] selAttrs_,
                      int nRelations, string[] relations_,
                      int nConditions, Condition[] conditions_,
                      int order, RelAttr orderAttr,
                      bool group, RelAttr groupAttr)
        {
            IsValid();
            int i = 0;

            RelAttr[] selAttrs = new RelAttr[nSelAttrs];
            for (i = 0; i < nSelAttrs; i++)
            {
                selAttrs[i].relName = selAttrs_[i].relName;
                selAttrs[i].attrName = selAttrs_[i].attrName;
            }

            AggRelAttr[] selAggAttrs = new AggRelAttr[nSelAttrs];
            for (i = 0; i < nSelAttrs; i++)
            {
                selAggAttrs[i].func = selAttrs_[i].func;
                selAggAttrs[i].relName = selAttrs_[i].relName;
                selAggAttrs[i].attrName = selAttrs_[i].attrName;
            }

            string[] relations = new string[nRelations];
            for (i = 0; i < nRelations; i++)
            {
                // strncpy(relations[i], relations_[i], MAXNAME);
                relations[i] = relations_[i];
            }

            Condition[] conditions = new Condition[nConditions];
            for (i = 0; i < nConditions; i++)
            {
                conditions[i] = conditions_[i];
            }

            if (CheckDuplicateConditions(relations)) throw new Exception();

            // rewrite select *
            if (nSelAttrs == 1 && selAttrs[0].attrName.Equals("*") == true)
            {
                nSelAttrs = 0;
                for (i = 0; i < nRelations; i++)
                {
                    var tuple = smm.GetFromTable(relations[i]);
                    int ac = tuple.Item1;
                    DataAttrInfo[] aa = tuple.Item2;
                    nSelAttrs += ac;
                }

                selAttrs = new RelAttr[nSelAttrs];
                selAggAttrs = new AggRelAttr[nSelAttrs];

                int j = 0;
                for (i = 0; i < nRelations; i++)
                {

                    var tuple2 = smm.GetFromTable(relations[i]);

                    int ac = tuple2.Item1;
                    DataAttrInfo[] aa = tuple2.Item2;
                    for (int k = 0; k < ac; k++)
                    {
                        selAttrs[j].attrName = aa[k].attrName;
                        selAttrs[j].relName = relations[i].ToArray();
                        selAggAttrs[j].attrName = aa[k].attrName;
                        selAggAttrs[j].relName = relations[i].ToArray();
                        selAggAttrs[j].func = Const.ConstProperty.AggFun.NO_F;
                        j++;
                    }
                }
            }

            if (order != 0)
            {
                smm.FindRelForAttr(orderAttr, nRelations, relations);
            }

            if (group)
            {
                smm.FindRelForAttr(groupAttr, nRelations, relations);
            }
            else
            {
                // make sure no agg functions are defined
                for (i = 0; i < nSelAttrs; i++)
                {
                    if (selAggAttrs[i].func != Const.ConstProperty.AggFun.NO_F)
                        throw new Exception();
                }
            }

            // rewrite select COUNT(*)
            for (i = 0; i < nSelAttrs; i++)
            {
                if (selAggAttrs[i].attrName.Equals("*") == true &&
                   selAggAttrs[i].func == Const.ConstProperty.AggFun.COUNT_F)
                {
                    selAggAttrs[i].attrName = groupAttr.attrName;
                    selAggAttrs[i].relName = groupAttr.relName;
                    selAttrs[i].attrName = groupAttr.attrName;
                    selAttrs[i].relName = groupAttr.relName;
                }
            }

            for (i = 0; i < nSelAttrs; i++)
            {
                if (selAttrs[i].relName == null)
                {
                    smm.FindRelForAttr(selAttrs[i], nRelations, relations);
                }
                else
                {
                    selAttrs[i].relName = selAttrs[i].relName;
                }
                selAggAttrs[i].relName = selAttrs[i].relName;
            }

            for (i = 0; i < nConditions; i++)
            {
                if (conditions[i].lhsAttr.relName == null)
                {
                    smm.FindRelForAttr(conditions[i].lhsAttr, nRelations, relations);
                }
                else
                {
                    conditions[i].lhsAttr.relName = conditions[i].lhsAttr.relName;
                }

                if (conditions[i].bRhsIsAttr == true)
                {
                    if (conditions[i].rhsAttr.relName == null)
                    {
                        smm.FindRelForAttr(conditions[i].rhsAttr, nRelations, relations);
                    }
                    else
                    {
                        conditions[i].rhsAttr.relName = conditions[i].rhsAttr.relName;
                    }
                }
            }

            // ensure that all relations mentioned in conditions are in the from clause
            for (i = 0; i < nConditions; i++)
            {
                bool lfound = false;
                for (int j = 0; j < nRelations; j++)
                {
                    if (conditions[i].lhsAttr.relName.Equals(relations[j]) == true)
                    {
                        lfound = true;
                        break;
                    }
                }
                if (!lfound) throw new Exception();

                if (conditions[i].bRhsIsAttr == true)
                {
                    bool rfound = false;
                    for (int j = 0; j < nRelations; j++)
                    {
                        if (conditions[i].rhsAttr.relName.Equals(relations[j]) == true)
                        {
                            rfound = true;
                            break;
                        }
                    }
                    if (!rfound) throw new Exception();
                }
            }

            OperationIterator it;

            if (nRelations == 1)
            {
                it = GetLeafIterator(relations[0], nConditions, conditions, 0, null, order, orderAttr);
                MakeRootIterator(it, nSelAttrs, selAggAttrs, nRelations, relations,
                                         order, orderAttr, group, groupAttr);
                PrintIterator(it);
            }

            if (nRelations >= 2)
            {
                //// Heuristic - join smaller operands first - sort relations by numRecords
                relations = ReArrangeRelations(relations,p=>smm.GetNumRecords(p));

                // Heuristic - left-deep join tree shape
                Condition[] lcond = GetCondsForSingleRelation(nConditions, conditions, relations[0]);
                it = GetLeafIterator(relations[0], lcond.Length, lcond, 0, null, order, orderAttr);

                for (i = 1; i < nRelations; i++)
                {
                    var jcond = GetCondsForTwoRelations(nConditions, conditions, i, relations, relations[i]);

                    var rcond = GetCondsForSingleRelation(nConditions, conditions, relations[i]);
                    var rfs = GetLeafIterator(relations[i], rcond.Length, rcond, jcond.Length,
                                                    jcond, order, orderAttr);

                    OperationIterator newit = null;

                    if (i == 1)
                    {
                        FileScan<TK> fit =it as FileScan<TK>;

                        if (it != null)
                        {
                            newit = new NestedBlockJoin<TK>(fit, rfs, jcond, jcond.Length);
                        }
                        else
                        {
                            newit = new NestedLoopJoin(it, rfs, jcond, jcond.Length);
                        }
                    }

                    IndexScan<TK> rixit = rfs as IndexScan<TK>;
                    IndexScan<TK> lixit = null;

                    // flag to see if index merge join is possible
                    int indexMergeCond = -1;
                    // look for equijoin (addl conditions are ok)
                    for (int k = 0; k < jcond.Length; k++)
                    {
                        if ((jcond[k].op == Const.ConstProperty.CompOp.EQ_OP) &&
                           (rixit != null) &&
                           (rixit.indexAttrName.Equals(jcond[k].lhsAttr.attrName) == true
                            || rixit.indexAttrName.Equals(jcond[k].rhsAttr.attrName) == true))
                        {
                            indexMergeCond = k;
                            break;
                        }
                    }
                    var mj = smm.Get("mergejoin");
                    if (mj == "no")
                        indexMergeCond = -1;

                    if (indexMergeCond > -1 && i == 1)
                    {
                        lcond = GetCondsForSingleRelation(nConditions, conditions, relations[0]);

                        it = GetLeafIterator(relations[0], lcond.Length, lcond, jcond.Length, jcond, order, orderAttr);

                        lixit = it as IndexScan<TK>;

                        if ((lixit == null) ||
                           (lixit.indexAttrName.Equals(jcond[indexMergeCond].lhsAttr.attrName)) != true &&
                           (lixit.indexAttrName.Equals(jcond[indexMergeCond].rhsAttr.attrName) != true))
                        {
                            indexMergeCond = -1;
                        }

                        if (lixit.desc != rixit.desc)
                        {
                            indexMergeCond = -1;
                        }
                    }

                    bool nlijoin = true;
                    var nlij = smm.Get("nlij");
                    if (nlij == "no")
                        nlijoin = false;

                    if (indexMergeCond > -1 && i == 1)
                    { //both have to be indexscans
                        newit = new MergeJoin(lixit, rixit, jcond.Length, indexMergeCond, jcond);
                    }
                    else
                    {
                        if (rixit != null && nlijoin)
                        {
                            newit = new NestedLoopIndexJoin<TK>(it, rixit, jcond, jcond.Length);
                        }
                        else
                        {
                            if (newit == null)
                            {
                                newit = new NestedLoopJoin(it, rfs, jcond,jcond.Length);
                            }
                        }
                    }

                    if (i == nRelations - 1)
                    {
                        MakeRootIterator(newit, nSelAttrs, selAggAttrs, nRelations, relations,
                                                 order, orderAttr, group, groupAttr);
                    }

                    it = newit;
                }

                PrintIterator(it);
            }
        }

        public void Insert(
            string relName,
            int nValues,
            Value[] values)
        {
            IsValid();

            var tuple = smm.GetFromTable(relName);
            int attrCount = tuple.Item1;
            DataAttrInfo[] attr = tuple.Item2;

            if (nValues != attrCount) throw new Exception();

            int size = 0;
            for (int i = 0; i < nValues; i++)
            {
                if (values[i].type != attr[i].attrType) throw new Exception();
                size += attr[i].attrLength;
            }

            char[] buf = new char[size];
            int offset = 0;
            for (int i = 0; i < nValues; i++)
            {
                for (int j = 0; j < attr[i].attrLength; j++)
                {
                    buf[offset + j] = values[i].value[j];
                }

                offset += attr[i].attrLength;
            }

            smm.LoadRecord(relName, size, new string(buf));
        }

        //
        // Delete from the relName all tuples that satisfy conditions
        //
        public void Delete(
            string relName,
            int nConditions,
            Condition[] conditions_
            )
        {
            IsValid();
            var tuple = smm.GetRelFromCat(relName);

            Condition[] conditions = new Condition[nConditions];
            for (int i = 0; i < nConditions; i++)
            {
                conditions[i] = conditions_[i];
            }

            for (int i = 0; i < nConditions; i++)
            {
                if (conditions[i].lhsAttr.relName == null)
                {
                    conditions[i].lhsAttr.relName = relName.ToArray();
                }
                if (conditions[i].lhsAttr.relName.Equals(relName) != true) throw new Exception();

                if (conditions[i].bRhsIsAttr == true)
                {
                    if (conditions[i].rhsAttr.relName == null)
                    {
                        conditions[i].rhsAttr.relName = relName.ToArray();
                    }
                    if (conditions[i].rhsAttr.relName.Equals(relName) != true) throw new Exception();
                }
            }

            var it = GetLeafIterator(relName, nConditions, conditions);

            var t = it.GetTuple();
            it.Open();

            RM_FileHandle fh = rmm.OpenFile(relName);
            
            var tupleTable = smm.GetFromTable(relName);
            int attrCount = tupleTable.Item1;
            DataAttrInfo[] attributes = tupleTable.Item2;

            IX_FileHandle<TK> indexes = new IX_FileHandle<TK>(fh.pfHandle, ConverTKToString,OccupiedNum);
            for (int i = 0; i < attrCount; i++)
            {
                if (attributes[i].indexNo != -1)
                {
                    indexes = ixm.OpenFile(relName, ixm.treeHeight);
                }
            }

            while (true)
            {
                it.GetNext(t);
                if (t== null)break;

                fh.DeleteRec(t.GetRid());

                for (int i = 0; i < attrCount; i++)
                {
                    if (attributes[i].indexNo != -1)
                    {
                        var t1 = t as DataTupleT<TK>;

                        var pKey = t1.GetData(attributes[i].offset, attributes[i].attrLength, ConvertStringToTK);

                        indexes.DeleteEntry(pKey);
                    }
                }
            }

            for (int i = 0; i < attrCount; i++)
            {
                if (attributes[i].indexNo != -1)
                {
                    ixm.CloseFile(indexes.iih);
                }
            }

            rmm.CloseFile(fh);

            it.Close();
        }

        public void Update(
            string relName,
            RelAttr updAttr_,
            bool bIsValue,
            RelAttr rhsRelAttr,
            Value rhsValue,
            int nConditions, 
            Condition[] conditions_)
        {
            IsValid();
            var tuple = smm.GetRelFromCat(relName);

            Condition[] conditions = new Condition[nConditions];
            for (int i = 0; i < nConditions; i++)
            {
                conditions[i] = conditions_[i];
            }

            RelAttr updAttr;
            updAttr.relName = relName.ToArray();
            updAttr.attrName = updAttr_.attrName;

            Condition cond;
            cond.lhsAttr = updAttr;
            cond.bRhsIsAttr = bIsValue?false :true;
            cond.rhsAttr.attrName = rhsRelAttr.attrName;
            cond.rhsAttr.relName = relName.ToArray();
            cond.op = Const.ConstProperty.CompOp.EQ_OP;
            cond.rhsValue.type = rhsValue.type;
            cond.rhsValue.value = rhsValue.value;

            if (bIsValue != true)
            {
                updAttr.attrName = rhsRelAttr.attrName;
            }

            for (int i = 0; i < nConditions; i++)
            {
                if (conditions[i].lhsAttr.relName == null)
                {
                    conditions[i].lhsAttr.relName = relName.ToArray();
                }
                if (conditions[i].lhsAttr.relName.Equals(relName) != true) throw new Exception();

                if (conditions[i].bRhsIsAttr == true)
                {
                    if (conditions[i].rhsAttr.relName == null)
                    {
                        conditions[i].rhsAttr.relName = relName.ToArray();
                    }
                    if (conditions[i].rhsAttr.relName.Equals(relName) != true) throw new Exception();
                }
            }

            OperationIterator it;
            // handle halloween problem by not choosing indexscan on an attr when the attr
            // is the one being updated.
            if (smm.IsAttrIndexed(new string(updAttr.relName), new string(updAttr.attrName)))
            {
                // temporarily make attr unindexed
                smm.DropIndexFromAttrCatAlone(new string(updAttr.relName), new string(updAttr.attrName));

                it = GetLeafIterator(relName, nConditions, conditions);

                smm.ResetIndexFromAttrCatAlone(new string(updAttr.relName), new string(updAttr.attrName));
            }
            else
            {
                it = GetLeafIterator(relName, nConditions, conditions);
            }

            var t = it.GetTuple();
            it.Open();

            char[] val;
            if (bIsValue == true)
                val = rhsValue.value;
            else
                val = t.Get(rhsRelAttr.attrName);

            RM_FileHandle fh = rmm.OpenFile(relName);
            int updAttrOffset = -1;
            
            var tuple2 = smm.GetFromTable(relName);
            int attrCount = tuple2.Item1;
            DataAttrInfo[] attributes = tuple2.Item2;

            IX_FileHandle<TK> indexes = new IX_FileHandle<TK>(fh.pfHandle, ConverTKToString, OccupiedNum);
            for (int i = 0; i < attrCount; i++)
            {
                if (attributes[i].indexNo != -1 &&
                   attributes[i].attrName.Equals(new string(updAttr.attrName)) == true)
                {
                    ixm.OpenFile(relName, ixm.treeHeight);
                }
                if (attributes[i].attrName.Equals(updAttr.attrName) == true)
                {
                    updAttrOffset = attributes[i].offset;
                }
            }

            while (true)
            {
                it.GetNext(t);
                if (t == null) break;

                RM_Record rec = new RM_Record();

                for (int i = 0; i < attrCount; i++)
                {
                    if (attributes[i].indexNo != -1 && attributes[i].attrName.Equals(updAttr.attrName) == true)
                    {
                        var t1 = t as DataTupleT<TK>;
                        var pKey = t1.GetData(attributes[i].offset, attributes[i].attrLength, ConvertStringToTK);

                        indexes.DeleteEntry(pKey);
                        indexes.InsertEntry(new RIDKey<TK>(t.rid,pKey));
                    }
                }

                t.Set(updAttrOffset,val);
                char[] newbuf = t.GetData();
                rec.Set(newbuf, it.DataTupleLength(), t.GetRid());
                fh.UpdateRec(rec);
            }

            it.Close();

            for (int i = 0; i < attrCount; i++)
            {
                if (attributes[i].indexNo != -1 &&
                   attributes[i].attrName.Equals(updAttr.attrName) == true)
                {
                    ixm.CloseFile(indexes.iih);
                }
            }

            rmm.CloseFile(fh);
        }

        private Condition[] GetCondsForSingleRelation(
            int nConditions,
            Condition[] conditions,
            string relName)
        {
            List<Condition> retConds = new List<Condition>();

            for (int j = 0; j < nConditions; j++)
            {
                if (conditions[j].bRhsIsAttr == true)
                    continue;
                if (conditions[j].lhsAttr.relName.Equals(relName) == true)
                {
                    retConds.Add(conditions[j]);
                }
            }
            return retConds.ToArray();
        }

        private Condition[] GetCondsForTwoRelations(
            int nConditions,
            Condition[] conditions,
            int nRelsSoFar,
            string[] relations,
            string relName2
            )
        {
            List<Condition> retConds = new List<Condition>();

            for (int i = 0; i < nRelsSoFar; i++)
            {
                string relName1 = relations[i];
                for (int j = 0; j < nConditions; j++)
                {
                    if (conditions[j].bRhsIsAttr == false)
                        continue;
                    if (conditions[j].lhsAttr.relName.Equals(relName1) == true &&
                        conditions[j].rhsAttr.relName.Equals(relName2) == true)
                    {
                        retConds.Add(conditions[j]);
                    }
                    if (conditions[j].lhsAttr.relName.Equals(relName2) == true
                       && conditions[j].rhsAttr.relName.Equals(relName1) == true)
                    {
                        retConds.Add(conditions[j]);
                    }
                }
            }

            return retConds.ToArray();
        }

        // Users will call - RC invalid = IsValid(); if(invalid) return invalid; 
        private bool IsValid()
        {
            bool ret = true;
            ret = ret && (smm.IsValid() == true);
            return ret ? true : false;
        }

        private bool strlt(char[] i, char[] j)
        {
            string str1 = new string(i);
            string str2 = new string(j);
            return (str1.CompareTo(str2) < 0);
        }

        private bool streq(char[] i, char[] j)
        {
            string str1 = new string(i);
            string str2 = new string(j);
            return (str1.CompareTo(str2) == 0);
        }

        private void MakeRootIterator(
            OperationIterator newit,
            int nSelAttrs, AggRelAttr[] selAttrs,
            int nRelations, string[] relations,
            int order, RelAttr orderAttr,
            bool group, RelAttr groupAttr)
        {
            if (order != 0)
            {
                // find the orderAttr
                smm.FindRelForAttr(orderAttr, nRelations, relations);
            }

            if (group)
            {
                newit = GetSortIterator(newit, nRelations, relations, order, groupAttr);

                AggRelAttr[] extraAttrs = new AggRelAttr[nSelAttrs];
                for (int i = 0; i < nSelAttrs; i++)
                {
                    extraAttrs[i] = selAttrs[i];
                }
                int nExtraSelAttrs = nSelAttrs;

                if (order != 0)
                {
                    // add the sort column as a projection just in case
                    newit = GetProjectionIterator(nSelAttrs, selAttrs, orderAttr, newit);
                }

                newit = new Agg(newit, groupAttr, nExtraSelAttrs, extraAttrs);
            }

            if (order != 0)
            {
                newit = GetSortIterator(newit, nRelations, relations, order, groupAttr);
            }

            newit = new Projection(newit, nSelAttrs, selAttrs);
        }

        private OperationIterator GetSortIterator(OperationIterator newit, 
            int nRelations, string[] relations, int order, RelAttr groupAttr)
        {
            bool desc = (order == -1) ? true : false;

            smm.FindRelForAttr(groupAttr, nRelations, relations);

            DataAttrInfo d = new DataAttrInfo();
            List<DataAttrInfo> pattr = newit.attrs;
            for (int i = 0; i < newit.attrs.Count; i++)
            {
                if (pattr[i].relName.Equals(groupAttr.relName) == true &&
                    pattr[i].attrName.Equals(groupAttr.attrName) == true)
                    d = pattr[i];
            }

            if (newit.bSorted &&
                newit.desc == desc &&
                newit.sortRel.Equals(new string(groupAttr.relName)) == true &&
                newit.sortAttr.Equals(new string(groupAttr.attrName)) == true)
            {
            }
            else
            {
                newit = new Sort(newit, d.attrType, d.attrLength, d.offset, desc);
            }

            return newit;
        }

        private OperationIterator GetProjectionIterator(int nSelAttrs, AggRelAttr[] selAttrs, RelAttr orderAttr,
            OperationIterator it)
        {
            int nExtraSelAttrs = nSelAttrs + 1;
            var extraAttrs = new AggRelAttr[nExtraSelAttrs];
            AggRelAttr[] extraAttrsNoF = new AggRelAttr[nExtraSelAttrs];

            for (int i = 0; i < nExtraSelAttrs - 1; i++)
            {
                extraAttrs[i] = selAttrs[i];
                extraAttrsNoF[i] = selAttrs[i];
                extraAttrsNoF[i].func = Const.ConstProperty.AggFun.NO_F;
            }

            extraAttrs[nExtraSelAttrs - 1].relName = orderAttr.relName;
            extraAttrs[nExtraSelAttrs - 1].attrName = orderAttr.attrName;
            extraAttrs[nExtraSelAttrs - 1].func = Const.ConstProperty.AggFun.NO_F;
            extraAttrsNoF[nExtraSelAttrs - 1] = extraAttrs[nExtraSelAttrs - 1];

            var newit = new Projection(it, nExtraSelAttrs, extraAttrsNoF);

            return newit;
        }

        private OperationIterator GetLeafIterator(
            string relName,
            int nConditions,
            Condition[] conditions,
            int nJoinConditions = 0,
            Condition[] jconditions = null,
            int order = 0,
            RelAttr porderAttr = default(RelAttr))
        {
            if (!IsValid()) throw new Exception();

            if (relName == null)
            {
                return null;
            }

            var tuple = smm.GetFromTable(relName);
            if (tuple == null) return null;

            DataAttrInfo[] attributes = tuple.Item2;
            int attrCount = tuple.Item1;

            Condition[] filters;
            int nFilters = -1;

            int nIndexes = 0;
            char[] chosenIndex = null;
            Condition chosenCond = default(Condition);
            var jBased = ReturnBasedCondition(relName, nJoinConditions, jconditions, attributes, attrCount,
                ref nIndexes, ref chosenIndex, ref chosenCond, true);

            if (chosenCond.CompareTo(default(Condition)) != 0)
            {
                nFilters = nConditions;
                filters = new Condition[nFilters];
                for (int j = 0; j < nConditions; j++)
                {
                    if (chosenCond.CompareTo(conditions[j]) != 0)
                    {
                        filters[j] = conditions[j];
                    }
                }
            }
            else
            {
                ReturnBasedCondition(relName, nJoinConditions, jconditions, attributes, attrCount,
                ref nIndexes, ref chosenIndex, ref chosenCond, false);

                if (chosenCond.CompareTo(default(Condition)) == 0)
                {
                    nFilters = nConditions;
                    filters = new Condition[nFilters];
                    for (int j = 0; j < nConditions; j++)
                    {
                        if (chosenCond.CompareTo(conditions[j]) != 0)
                        {
                            filters[j] = conditions[j];
                        }
                    }
                }
                else
                {
                    nFilters = nConditions - 1;
                    filters = new Condition[nFilters];
                    for (int j = 0, k = 0; j < nConditions; j++)
                    {
                        if (chosenCond.CompareTo(conditions[j]) != 0)
                        {
                            filters[k] = conditions[j];
                            k++;
                        }
                    }
                }
            }

            if (chosenCond.CompareTo(default(Condition)) == 0 && (nConditions == 0 || nIndexes == 0))
            {
                OperationIterator it;
                if (nConditions == 0)
                    it = new FileScan<TK>(rmm, smm, relName, default(Condition), 0, null);
                else
                    it = new FileScan<TK>(rmm, smm, relName, default(Condition), nConditions,
                                      conditions);
                return it;
            }

            OperationIterator itIndex;

            bool desc = false;
            string str1 = new string(porderAttr.relName);
            string str2 = new string(porderAttr.attrName);
            if (order != 0 &&
               str1.Equals(relName) == true &&
               str2.Equals(chosenIndex) == true)
                desc = (order == -1 ? true : false);

            string chosenIndexStr = new string(chosenIndex);
            if (chosenCond.CompareTo(default(Condition)) != 0)
            {
                if (chosenCond.op == Const.ConstProperty.CompOp.EQ_OP ||
                   chosenCond.op == Const.ConstProperty.CompOp.GT_OP ||
                   chosenCond.op == Const.ConstProperty.CompOp.GE_OP)
                    if (order == 0) // use only if there is no order-by
                        desc = true; // more optimal

                itIndex = new IndexScan<TK>(smm, rmm, ixm, relName, chosenIndexStr,
                                   chosenCond, nFilters, filters, desc, ixm.treeHeight);
            }
            else // non-conditional index scan
                itIndex = new IndexScan<TK>(smm, rmm, ixm, relName, chosenIndexStr,
                                   default(Condition), nFilters, filters, desc, ixm.treeHeight);

            return itIndex;

        }

        private void GetCondsForSingleRelation(
            int nConditions,
            Condition[] conditions,
            string relName, 
            Condition[] retConds,
            ref int retCount)
        {
            List<int> v = new List<int>();

            for (int j = 0; j < nConditions; j++)
            {
                if (conditions[j].bRhsIsAttr == true)
                    continue;
                string str = new string(conditions[j].lhsAttr.relName);
                if (str.Equals(relName) == true)
                {
                    v.Add(j);
                }
            }

            retCount = v.Count();
            if (retCount == 0)
                return;
            retConds = new Condition[retCount];
            for (int i = 0; i < retCount; i++)
                retConds[i] = conditions[v[i]];
            return;
        }

        private void GetCondsForTwoRelations(
           int nConditions,
           Condition[] conditions,
           int nRelsSoFar,
           string[] relations,
           string relName2,
           Condition[] retConds,
           ref int retCount)
        {
            List<int> v = new List<int>();

            for (int i = 0; i < nRelsSoFar; i++)
            {
                var relName1 = relations[i];
                for (int j = 0; j < nConditions; j++)
                {
                    if (conditions[j].bRhsIsAttr == false)
                        continue;
                    string lstr = new string(conditions[j].lhsAttr.relName);
                    string rstr = new string(conditions[j].rhsAttr.relName);

                    if (lstr.Equals(relName1) == true
                       && rstr.Equals(relName2) == true)
                    {
                        v.Add(j);
                    }
                    if (lstr.Equals(relName2) == true
                       && rstr.Equals(relName1) == true)
                    {
                        v.Add(j);
                    }
                }
            }

            retCount = v.Count();
            if (retCount == 0)
                return;
            retConds = new Condition[retCount];
            for (int i = 0; i < retCount; i++)
                retConds[i] = conditions[v[i]];
            return;
        }

        private static Condition ReturnBasedCondition(
            string relName, int nJoinConditions, 
            Condition[] jconditions, 
            DataAttrInfo[] attributes, 
            int attrCount, 
            ref int nIndexes, ref char[] chosenIndex, ref Condition chosenCond,bool IsReturn)
        {
            Condition jBased = default(Condition);
            Dictionary<string, Condition> jkeys = GetJkeys(relName, nJoinConditions, jconditions);

            foreach (var s in jkeys.Keys)
            {
                for (int i = 0; i < attrCount; i++)
                {
                    if (attributes[i].indexNo != -1 && (s.Equals(attributes[i].attrName) == true))
                    {
                        nIndexes++;
                        if (chosenIndex == null ||
                            attributes[i].attrType == Const.ConstProperty.AttrType.INT ||
                            attributes[i].attrType == Const.ConstProperty.AttrType.FLOAT)
                        {
                            chosenIndex = attributes[i].attrName;
                            chosenCond = jBased;

                            if (IsReturn)
                            {
                                jBased = jkeys[s];
                                jBased.lhsAttr.attrName = chosenIndex;
                                jBased.bRhsIsAttr = false;
                                jBased.rhsValue.type = attributes[i].attrType;
                                jBased.rhsValue.value = null;
                            }
                        }
                    }
                }
            }

            return jBased;
        }

        private static Dictionary<string, Condition> GetJkeys(string relName, int nJoinConditions, Condition[] jconditions)
        {
            Dictionary<string, Condition> jkeys = new Dictionary<string, Condition>();

            for (int j = 0; j < nJoinConditions; j++)
            {
                string lstr = new string(jconditions[j].lhsAttr.relName);
                if (lstr.Equals(relName) == true)
                {
                    jkeys[new string(jconditions[j].lhsAttr.attrName)] = jconditions[j];
                }

                string rstr = new string(jconditions[j].rhsAttr.relName);
                if (jconditions[j].bRhsIsAttr == true && rstr.Equals(relName) == true)
                {
                    jkeys[new string(jconditions[j].rhsAttr.attrName)] = jconditions[j];
                }
            }

            return jkeys;
        }

        string[] ReArrangeRelations(string[] relations,Func<string,int> func)
        {
            Dictionary<string, int> dic = new Dictionary<string, int>();
            foreach (var r in relations)
            {
                dic.Add(r, func(r));
            }

            return dic.OrderBy(t => t.Value).Select(p => p.Key).ToArray();
        }

        bool CheckDuplicateConditions(string[] relations)
        {
            var array1 = relations.OrderBy(p => p).ToArray();

            for (int i = 0; i < array1.Length; i++)
            {
                if (array1[i].Equals(array1[array1.Length - i]))
                    return true;
            }

            return false;
        }
    }
}
