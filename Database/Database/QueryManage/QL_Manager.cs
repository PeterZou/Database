﻿using Database.IndexManage;
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

        //
        // Constructor for the QL Manager
        //
        public QL_Manager(SM_Manager<TK> smm_, IX_Manager<TK> ixm_, RM_Manager rmm_)
        {
            this.smm = smm_;
            this.ixm = ixm_;
            this.rmm = rmm_;
        }

        public void PrintIterator(OperationIterator it)
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        private OperationIterator GetLeafIterator(
            string relName,
            int nConditions,
            Condition[] conditions,
            int nJoinConditions,
            Condition[] jconditions,
            int order,
            RelAttr porderAttr)
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
    }
}
