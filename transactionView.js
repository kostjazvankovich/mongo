db.transactionView.drop()
db.runCommand({
    create: "transactionView",
    viewOn: "Transactions",
    pipeline: [
      { 
        $facet: 
        {
          "lastActiveTransactions" : [
            {
              $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
            },
            {
              $group:
              {
                _id:
                {
                  clientId: "$clientId",
                  accountId: "$accountId",
                },
                lastBookingDate: { $last: "$bookingDate"},
                bookingDateClosingBalance: { $last: "$bookingDateClosingBalance"}
              }
            },
            {
              $project:
              {
                _id: 0,
                bookingDate: "$lastBookingDate",
                clientId: "$_id.clientId",
                accountId: "$_id.accountId",
                bookingDateClosingBalance: "$bookingDateClosingBalance"
              }
            },
            {
              $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
            }]
        }}
]});
