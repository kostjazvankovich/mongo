db.runCommand({
    create:   "bookedBalanceView",
    viewOn:   "bookedBalance", 
    pipeline: [
        {$match: {clientId: "465224-1551"}},
        {$unwind: {path: "$saldenList"}},
        {
            $project: {
                postenNr: "$saldenList.postenId.postenNr",
                assets: "$saldenList.details",
            }
        },
        { $unwind: {path: "$assets"} },
        {
            $project: {
                _id: 0,
                postenNr: 1,
                balances: "$assets.dailyBalances",
                bank: "$assets.displayText",
            }
        },
        { $unwind: {path: "$balances"} },
        {
            $match: {
                "balances.day": { "$gte": new Date((new Date()).valueOf() - ( 84 * ( 1000 * 60 * 60 * 24 ) ) ) }
            }
        },
        {
            $project:  {
                postenNr: 1,
                bank: 1,
                day: "$balances.day",
                value: "$balances.saldoInCent"
            }
        },
        {
            $group: {
                _id: {
                    postenNr: "$postenNr",
                    day: { 
                        $dateFromParts: {
                            year: { $year: "$day"},
                            month: { $month: "$day"},
                            day: { $dayOfMonth: "$day"}
                        }
                    }
                },
                banks: { $addToSet: "$bank"},
                total: { $sum: "$value"}
            }
        },
        {
            $facet: {
                banks: [
                    { $match: {$or: [{ "_id.postenNr": 16730000 }, { "_id.postenNr": 25210000 }]}},
                    {
                        $project: {
                            _id: 0,
                            banks: 1,
                            day: "$_id.day",
                            total: 1
                        }
                    },
                    {
                        $sort: { day: 1 }
                    }
                ],
                debts: [
                    { $match: { $or: [ { "_id.postenNr": 16730000 }, { "_id.postenNr": 25210000 }, { "_id.postenNr": 15610106 } ] } },
                    {
                        $group: {
                            _id: "$_id.day",
                            total: { $sum: "$total"}
                        }
                    },
                    {
                        $project: {
                            _id: 0,
                            day: "$_id",
                            total: 1
                        }
                    },
                    {
                        $sort: { day: 1 }
                    }
                ],
                demands: [
                    { $match: { $or: [ { "_id.postenNr": 16730000 }, { "_id.postenNr": 25210000 }, { "_id.postenNr": 15610106 }, { "_id.postenNr": 25411700 } ] } },
                    {
                        $group: {
                            _id: "$_id.day",
                            total: {
                                $sum: { 
                                    $cond: [
                                        {$or: [ { $eq: [ "$_id.postenNr", 25411700 ] }, { $eq: [ "$_id.postenNr", 25210000 ] } ]},
                                        { "$subtract": [ 0, "$total" ] },
                                        "$total"
                                    ]
                                }
                            }
                        }
                    },
                    {
                        $project: {
                            _id: 0,
                            day: "$_id",
                            total: 1
                        }
                    },
                    {
                        $sort: { day: 1 }
                    }
                ]
            }
        }
    ]
});
