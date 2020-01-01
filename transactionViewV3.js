db.transactionViewV2.drop()
db.runCommand(
  {
    create: "transactionViewV2",
    viewOn: "client",
    pipeline: [
      {
        $lookup:
          {
            from: "Transactions",
            localField: "clientId",
            foreignField: "clientId",
            as: "transactions"
          }
      },
      {
        $unwind: "$transactions"
      },
      {
        $project: {
          _id: 0,
          clientId: "$transactions.clientId",
          accountId: "$transactions.accountId",
          bookingDate: "$transactions.bookingDate",
          bookingDateClosingBalance: "$transactions.bookingDateClosingBalance"
        }
      },
      {
        $group:
          {
            _id:
              {
                clientId: "$clientId",
                accountId: "$accountId",
                bookingDate: "$bookingDate",
                bookingDateClosingBalance: "$bookingDateClosingBalance"
              }
          }
      },
      {
        $group:
          {
            _id:
              {
                clientId: "$_id.clientId",
                bookingDate: "$_id.bookingDate",
              },
            bookingDateClosingBalance: { $sum: "$_id.bookingDateClosingBalance" },
            accounts: { $addToSet: "$_id.accountId"}
          }
      },
      {
        $sort: {"_id.clientId": 1,  "_id.bookingDate": -1}
      },
      {
        $project:
          {
            _id: 0,
            clientId: "$_id.clientId",
            bookingDate: "$_id.bookingDate",
            bookingDateClosingBalance: 1,
            accounts: 1
          }
      },
      {
        $group:
          {
            _id: {clientId: "$clientId"},
            transactions: {$push: "$$ROOT"}
          }
      },
      {
        $project:
          {
            _id: 0,
            clientId: "$_id.clientId",
            transactions: { $slice: ["$transactions", 14]},
          }
      },
      {
        $project:
          {
            clientId: 1,
            transactions: 1,
            accounts: {
              $reduce: {
                input: "$transactions.accounts",
                initialValue: [],
                in: { "$setUnion": ["$$value", "$$this"] }
              }
            }
          }
      },
      {
        $lookup:
          {
            from: "Transactions",
            let: { clientId: "$clientId", accounts: "$accounts" },
            pipeline: [
              { $match: {
                  $expr: {
                    $and: [
                      { $eq: [ "$clientId",  "$$clientId" ] },
                      { $eq: [{ $setIsSubset: [ ["$accountId"], "$$accounts" ] }, false]}
                    ]
                  }
                 },
              },
              { $sort: {"bookingDate": -1} },
              { $limit: 1 },
              {
                $project:
                  {
                    _id: 0,
                    bookingDateClosingBalance: 1,
                  }
              }],
            as: "transaction"
          }
      },
      {
        $project:
          {
            clientId: 1,
            transactions: {
              $let: { 
                vars: { accountBalance: { $ifNull: [ { $arrayElemAt: ["$transaction", 0]}, 0]}},
                  in: {
                    $reduce: {
                      input: "$transactions",
                      initialValue: [],
                        in: 
                      { $concatArrays : ["$$value", [
                        {
                          bookingDate: "$$this.bookingDate",
                          bookingDateClosingBalance: { $add: ["$$this.bookingDateClosingBalance", "$$accountBalance"]}
                        }]]}
                    }}}
            }
          }
      },
      { $unwind: "$transactions" },
      {
        $group:
          {
            _id:
              {
                clientId: "$clientId",
                bookingDate: "$transactions.bookingDate"
              },
            bookingDateClosingBalance: { $sum: "$transactions.bookingDateClosingBalance" },
          }
      },
      {
        $project:
          {
            _id: 0,
            clientId:  "$_id.clientId",
            bookingDate: "$_id.bookingDate",
            bookingDateClosingBalance: 1,
          }
      },
      {
        $sort: {"clientId": 1,  "bookingDate": 1}
      },
      {
        $group:
         {
           _id:
             {
               clientId: "$clientId",
             },
           lastBookingDate: {$last: "$bookingDate"},
           lastBookingDateClosingBalance: { $last: "$bookingDateClosingBalance" },
           transactions: { $push: { bookingDate: "$bookingDate", bookingDateClosingBalance: "$bookingDateClosingBalance"}}
         }
      },
      {
        $project:
        {
          _id: 0,
          clientId: "$_id.clientId",
          transactions: 
          {
            $let: {
              vars: { transactions: "$transactions",
                      dateRange: { $range:[0, 15*1000*60*60*24, 1000*60*60*24] }, 
                      lastBookingDate: "$lastBookingDate",
                      lastBookingDateClosingBalance: "lastBookingDateClosingBalance"
                    },
                in: {
                  $reduce: {
                    input: "$$dateRange",
                    initialValue: [],
                      in: { $concatArrays: ["$$value", [ 
                        { $let: { 
                          vars: { rangeBookingDate: { $subtract: ["$$lastBookingDate", "$$this"]}
                                },
                            in: { $arrayElemAt: [{ $filter: { 
                              input: "$$transactions", cond: { $eq: ["$$this.bookingDate", "$$rangeBookingDate"]}}}, 0]}}
                        }
                      ]]}
                  }
                }
            }
          }
        }
      }
    ]
  })
