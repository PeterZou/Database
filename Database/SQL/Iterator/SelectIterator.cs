﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQL.Iterator
{
    public class SelectIterator : Iterator
    {
        Iterator Array { get; set; }
        public void Close()
        {
            throw new NotImplementedException();
        }

        public object GetNext()
        {
            throw new NotImplementedException();
        }

        public void Open()
        {
            Array.Open();
        }
    }
}