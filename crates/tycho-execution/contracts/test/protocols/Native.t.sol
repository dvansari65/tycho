pragma solidity ^0.8.26;

import "../TychoRouterTestSetup.sol";
import {NativeExecutor} from "@src/executors/NativeExecutor.sol";
import {Constants} from "../Constants.sol";
import "forge-std/Test.sol";

contract MockNativeTarget {
    IERC20 constant USDC =
        IERC20(0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48);
    uint256 constant AMOUNT_IN = 3000000000;

    receive() external payable {}
    fallback() external payable {
        // Simulate Native settlement: the target spends the router-approved
        // input token, then sends the quoted output back to the router.
        USDC.transferFrom(msg.sender, address(this), AMOUNT_IN);
        payable(msg.sender).transfer(1000);
    }
}

contract TychoRouterForNativeTest is TychoRouterTestSetup {
    function getForkBlock() public pure override returns (uint256) {
        return 22644371; 
    }

    function testSingleSwap() public {
        IERC20 USDC = IERC20(USDC_ADDR);
        // The amount in our Rust test was 3000000000 (3000 USDC)
        uint256 amountIn = 3000000000;
        
        deal(address(USDC), ALICE, amountIn);
        uint256 balanceBefore = ALICE.balance;
        
        vm.startPrank(ALICE);
        USDC.approve(tychoRouterAddr, type(uint256).max);
        
        // Target address used in the rust unit test
        address target = 0xb2d1F342D2049684Fb2f8c4eF320633415598333;
        MockNativeTarget mock = new MockNativeTarget();
        vm.etch(target, address(mock).code); 
        deal(target, 10 ether); 
        
        bytes memory callData = loadCallDataFromFile("test_single_encoding_strategy_native");

        (bool success,) = tychoRouterAddr.call(callData);

        uint256 balanceAfter = ALICE.balance;
        assertTrue(success, "Call Failed");
        assertEq(balanceAfter - balanceBefore, 1000);
        assertEq(USDC.balanceOf(tychoRouterAddr), 0);
    }
}

contract NativeExecutorTest is Test, Constants {
    NativeExecutor executor;

    function setUp() public {
        executor = new NativeExecutor();
    }

    function testGetTransferData() public {
        address target = 0xb2d1F342D2049684Fb2f8c4eF320633415598333;
        address tokenIn = USDC_ADDR;
        address tokenOut = ETH_ADDR;
        uint256 value = 0;
        bytes memory payload = hex"123456";
        
        // 92 bytes header
        bytes memory params = abi.encodePacked(
            bytes20(tokenIn),
            bytes20(tokenOut),
            bytes20(target),
            bytes32(value),
            payload
        );
        
        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenInDecoded,
            address tokenOutDecoded,
            bool outputToRouter
        ) = executor.getTransferData(params);
        
        assertEq(uint8(transferType), uint8(TransferManager.TransferType.ProtocolWillDebit));
        assertEq(receiver, target);
        assertEq(tokenInDecoded, tokenIn);
        assertEq(tokenOutDecoded, tokenOut);
        assertEq(outputToRouter, true);
    }

    function testGetTransferDataNativeETH() public {
        address target = 0xb2d1F342D2049684Fb2f8c4eF320633415598333;
        address tokenIn = 0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE; // ETH_ADDRESS
        address tokenOut = USDC_ADDR;
        uint256 value = 1 ether;
        bytes memory payload = hex"123456";
        
        bytes memory params = abi.encodePacked(
            bytes20(tokenIn),
            bytes20(tokenOut),
            bytes20(target),
            bytes32(value),
            payload
        );
        
        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenInDecoded,
            address tokenOutDecoded,
            bool outputToRouter
        ) = executor.getTransferData(params);
        
        assertEq(uint8(transferType), uint8(TransferManager.TransferType.TransferNativeInExecutor));
        assertEq(receiver, address(0));
        assertEq(tokenInDecoded, tokenIn);
        assertEq(tokenOutDecoded, tokenOut);
        assertEq(outputToRouter, true);
    }
}
