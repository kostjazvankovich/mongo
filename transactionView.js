db.transactionView.drop()
db.runCommand({
  create: "transactionView",
  viewOn: "Transactions",
  pipeline: [
    { 
       $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1} 
    },
    {
      $group:
      { _id: 
        {
          clientId: "$clientId",
          accountId: "$accountId",
          bookingDate: "$bookingDate",
//                bookingDateClosingBalance: "$bookingDateClosingBalance"
        },
        bookingDateClosingBalance: { $last: "$bookingDateClosingBalance"}
      }
    },
    {
      $project:
      {
        clientId: "$_id.clientId",
	accountId: "$_id.accountId",
        bookingDate:
        {
          $cond: [ {
            $gte:
            [
              "$_id.bookingDate",
              {
                $let:
                {
                  vars: { startDate: { $subtract: [new ISODate(), 1209600000]}},
                    in: "$$startDate"
                }
              }
            ]
          }, "$_id.bookingDate", { $subtract: [new ISODate(), 1209600000]} ]
        },
        bookingDateClosingBalance: "$bookingDateClosingBalance"
      }
    },
    // { 
    //   $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1, "_id.bookingDate": 1} 
    // },
    // {
    //   $group:
    //   { 
    //     _id: 
    //     {
    //       clientId: "$clientId",
    //       accountId: "$accountId",
    //       bookingDate: "$bookingDate"
    //     },
    //     bookingDateClosingBalances: 
    //     { 
    //       $cond: [ {
    //         $gte:
    //         [
    //           "$_id.bookingDate",
    //           {
    //             $let:
    //             {
    //               vars: { startDate: { $subtract: [new ISODate(), 1209600000]}},
    //                 in: "$$startDate"
    //             }
    //           }
    //         ]
    //       }, "$_id.bookingDateClosingBalance", { $last: "$_id.bookingDateClosingBalance"} ]
    //     }
    //   }
    // },
    {
      $group:
      { 
        _id: 
        {
          clientId: "$clientId",
          accountId: "$accountId",
          bookingDate: "$bookingDate"
        },
        bookingDateClosingBalances: 
        { 
          $push: "$bookingDateClosingBalance"
        }
      }
    },
    {
      $project:
      {
        _id: 0,
        clientId: "$_id.clientId",
	accountId: "$_id.accountId",
        bookingDate: "$_id.bookingDate",
        bookingDateClosingBalances: "$bookingDateClosingBalances"
      }
    },
    { 
      $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1} 
    },
    // {
    //   $addFields:
    //   {
    //     rangeDate:
    //     {
    //       $map:
    //       { 
    //         input:{$range:[0, 15*1000*60*60*24, 1000*60*60*24]},
    //           in:{$subtract:[new ISODate(), "$$this"]}
    //       }
    //     }
    //   }
    // },
    // { $unwind: "$rangeDate"},
    // {
    //   $group:
    //   {
    //     _id: 
    //     {
    //       clientId: "$clientId",
    //       accountId: "$accountId",
    //       rangeDate: "$rangeDate"
    //     },
    //     bookingDateClosingBalance: { $push: "$bookingDateClosingBalance"}
    //   }
    // },
    // { $sort:{ "_id.clientId": 1, "_id.accountId": 1, "_id.rangeDate": 1} },
    // {
    //   $project: {
    //     _id: 0,
    //     rangeDate: "$_id.rangeDate",
    //     clientId: "$_id.clientId",
    //     accountId: "$_id.accountId",
    //     bookingDateClosingBalance: "$bookingDateClosingBalance"
    //     // bookingDateClosingBalance:
    //     // {
    //     //   $cond: [ {
    //     //     $gte:
    //     //     [
    //     //       "$_id.bookingDate",
    //     //       {
    //     //         $let:
    //     //         {
    //     //           vars: { startDate: { $subtract: [new ISODate(), 1209600000]}},
    //     //             in: "$$startDate"
    //     //         }
    //     //       }
    //     //     ]
    //     //   }, "$bookingDateClosingBalance", { $last: "$bookingDateClosingBalance"} ]
    //     // }
    //   }
    //    }
]});
