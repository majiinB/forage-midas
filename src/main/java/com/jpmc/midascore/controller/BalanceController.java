package com.jpmc.midascore.controller;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.component.DatabaseConduit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class BalanceController {

    @Autowired
    private DatabaseConduit databaseConduit;

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam("userId") long userId) {
        UserRecord user = databaseConduit.findUserById(userId);
        float balance = (user != null) ? user.getBalance() : 0f;
        return new Balance(balance);
    }
}
