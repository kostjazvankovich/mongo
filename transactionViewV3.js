db.transactionView.drop()
db.runCommand(
  {
    create: "transactionView",
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
            accounts: { $addToSet: { accountId: "$_id.accountId", bookingDate: "$_id.bookingDate", bookingDateClosingBalance: "$_id.bookingDateClosingBalance" }}
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
        $unwind: "$transactions"
      },
      {
        $replaceRoot: { newRoot: "$transactions" }
      },
      {
        $sort: { clientId: 1, bookingDate: 1 }
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
                      { $eq: [{ $setIsSubset: [ ["$accountId"], "$$accounts.accountId" ] }, false]}
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
            bookingDate: 1,
            bookingDateClosingBalance: { $add: ["$bookingDateClosingBalance", {  $ifNull: [{$arrayElemAt: ["$transaction.bookingDateClosingBalance", 0]}, 0]}]}
          }
      },
      {
        $sort: { clientId: 1, bookingDate: 1}
      },
      {
        $group:
          {
            _id:
              {
                clientId: "$clientId",
              },
            lastBookingDate: { $last: "$bookingDate" },
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
                              vars: { rangeBookingDate: { $subtract: ["$$lastBookingDate", "$$this"]}},
                              in: { $let: {
                                  vars: {
                                    balance: {
                                      $let: {
                                        vars: { },
                                        in: { $ifNull: [{ $arrayElemAt: [{ $filter: {
                                                input: "$$transactions", cond: { $eq: ["$$this.bookingDate", "$$rangeBookingDate"]}}}, 0]},
                                            { $ifNull: [{ $arrayElemAt: [{ $filter: {
                                                    input: "$$transactions", cond: { $lt: ["$$this.bookingDate", "$$rangeBookingDate"]}}}, -1]},
                                                { $arrayElemAt: [{ $filter: {
                                                      input: "$$transactions", cond: { $gt: ["$$this.bookingDate", "$$rangeBookingDate"]}}}, 0]}]}]}}}},
                                  in: {
                                    bookingDate: "$$rangeBookingDate",
                                    bookingDateClosingBalance: "$$balance.bookingDateClosingBalance"
                                  }
                                }
                              }
                            }
                          }
                        ]]}
                    }
                  }
                }
              }
          }
      },
      {
        $unwind: "$transactions"
      },
      {
        $replaceRoot: { newRoot:
            {
              clientId: "$clientId",
              bookingDate: "$transactions.bookingDate",
              total: {$divide:[
                  {$subtract:[
                      {$multiply:['$transactions.bookingDateClosingBalance',100]},
                      {$mod:[{$multiply:['$transactions.bookingDateClosingBalance',100]}, 1]}
                    ]},
                  100
                ]}
            }
        }
      },
      {
        $sort: { clientId: 1, bookingDate: 1 }
      }
    ]
  })
