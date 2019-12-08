db.lastActiveTransactionView.drop()
db.runCommand({
    create: "activeTransactionView",
    viewOn: "Transactions",
    pipeline: [
        {
            $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
        },
        {
            $group:
                {
                    _id:
                        {
                            clientId: "$clientId",
                            accountId: "$accountId"
                        },
                    bookingDate: { $last: "$bookingDate"},
                    bookingDateClosingBalance: { $last: "$bookingDateClosingBalance"}
                }
        },
        {
            $project:
                {
                    _id: 0,
                    clientId: "$_id.clientId",
                    accountId: "$_id.accountId",
                    bookingDate: "$bookingDate",
                    bookingDateClosingBalance: "$bookingDateClosingBalance"
                }
        },
        {
            $sort:{ "clientId": 1, "accountId": 1, "bookingDate": 1}
        }
    ]});
