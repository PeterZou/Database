﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.BufferManage
{
    public class PF_BufPageDesc
    {
        public char[] data = new char[ConstProperty.PF_FILE_HDR_SIZE];
        public bool dirty; //true if has modified, false if not or with init
        public int pinCount; //whether used or not
        public int fd; //fd must start from 1;
        public int pageNum;
        public int slotSequence;

        public override string ToString()
        {
            return string.Format("This page des is as follows. fd is: {0}, pageNum is {1}, slot is {2}, data is {3}, dirty is {4}, pinCount is {5}",
                fd, pageNum, slotSequence, data, dirty, pinCount);
        }
    }
}
