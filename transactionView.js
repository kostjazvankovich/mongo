db.transactionView.drop()
db.runCommand({
    create: "transactionView",
    viewOn: "Transactions",
    pipeline: [
        {
            $lookup:
                {
                    from: "lastActiveTransactionView",
                    let: { bookingDate: "$bookingDate", accountId: "$accountId", clientId: "$clientId" },
                    pipeline: [
                        { $match:
                                { $expr:
                                        {  $and: [
                                                { $eq: [ "$clientId",  "$$clientId" ] },
                                                {
                                                    $gte:
                                                        [
                                                            "$$bookingDate",
                                                            {
                                                                $let:
                                                                    {
                                                                        vars:
                                                                            {
                                                                                startDate: {$subtract: ["$lastBookingDate", 1209600000]}
                                                                            }, in: "$$startDate"
                                                                    }
                                                            }
                                                        ]
                                                }]
                                        }
                                }
                        },
                        { $project: { _id: 0 } }
                    ],
                    as: "activeTransactions"
                }
        },
        {
            $unwind: "$activeTransactions"
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
                        },
                }
        },
        {
            $project:
            {
                _id: 0,
                clientId: "$_id.clientId",
                accountId: "$_id.accountId",
                bookingDate: "$_id.bookingDate",
                bookingDateClosingBalance: "$_id.bookingDateClosingBalance"
            }
        },
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
                startDate: { $first: "$bookingDate"},
                endDate: { $last: "$bookingDate"},
                bookingBalances: { $push: { bookingDate: "$bookingDate", bookingDateClosingBalance: "$bookingDateClosingBalance" }}
            }
        },
        {
            $project:
            {
                _id: 0,
                clientId: "$_id.clientId",
                accountId: "$_id.accountId",
                startDate: "$startDate",
                endDate: "$endDate",
                bookingBalances: "$bookingBalances"
            }
        },
    ]});
